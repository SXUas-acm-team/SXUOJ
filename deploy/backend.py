#!/usr/bin/env python3
"""Release one existing Compose backend, keeping immutable rollback snapshots."""

import argparse
import base64
import copy
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from contextlib import contextmanager


class DeployError(RuntimeError):
    pass


def docker(*args, stdin=None):
    # Compose diagnostics can contain interpolated credentials. Never echo them.
    try:
        result = subprocess.run(["docker", *args], input=stdin, text=True,
                                encoding="utf-8", capture_output=True)
    except OSError as exc:
        raise DeployError("Cannot execute Docker; install Docker Engine and Compose v2.") from exc
    if result.returncode:
        raise DeployError("Docker command failed (%s, exit %s); inspect Docker locally."
                          % (" ".join(args[:2]), result.returncode))
    return result.stdout.strip()


def inspect(kind, identifier):
    items = json.loads(docker(kind, "inspect", identifier))
    if len(items) != 1:
        raise DeployError("Expected exactly one Docker object.")
    return items[0]


def env_map(entries):
    return dict(item.split("=", 1) for item in entries or [] if "=" in item)


def validate_secrets(env):
    def first(*names):
        return next((str(env[name]).replace("$$", "$") for name in names if env.get(name)), "")

    jwt = first("HOJ_JWT_SECRET", "JWT_TOKEN_SECRET")
    try:
        valid_jwt = len(base64.b64decode(jwt + "=" * (-len(jwt) % 4), validate=True)) >= 64
    except (ValueError, TypeError):
        valid_jwt = False
    if not valid_jwt:
        raise DeployError("JWT_TOKEN_SECRET must be Base64 encoding at least 64 random bytes.")
    if len(first("HOJ_JUDGE_TOKEN", "JUDGE_TOKEN")) < 32:
        raise DeployError("JUDGE_TOKEN must contain at least 32 characters and match JudgeServer.")
    if first("NACOS_ENABLED").lower() == "false":
        raise DeployError("This production deployment requires the existing Nacos configuration.")
    if len(first("NACOS_PASSWORD")) < 12:
        raise DeployError("NACOS_PASSWORD must match the existing Nacos account (at least 12 characters).")


def literal_compose(value):
    # JSON is valid Compose YAML; escape literals before Compose reads the snapshot.
    if isinstance(value, str):
        return value.replace("$", "$$")
    if isinstance(value, list):
        return [literal_compose(item) for item in value]
    if isinstance(value, dict):
        return {key: literal_compose(item) for key, item in value.items()}
    return value


def write_private(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


@contextmanager
def release_lock(directory):
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    lock = directory / "lock"
    try:
        lock.mkdir(mode=0o700)
    except FileExistsError as exc:
        raise DeployError("Another release is active (or a stale lock exists): " + str(lock)) from exc
    try:
        yield
    finally:
        lock.rmdir()


class Deployment:
    def __init__(self, args):
        self.args = args
        self.files = [Path(name).resolve(strict=True) for name in args.compose]
        self.directory = self.files[0].parent
        self.env_files = [Path(name).resolve(strict=True) for name in args.env_file]
        self.common = ["compose", "--project-directory", str(self.directory)]
        for path in self.env_files:
            self.common += ["--env-file", str(path)]
        if args.project_name:
            self.common += ["--project-name", args.project_name]
        source_args = [part for path in self.files for part in ("-f", str(path))]
        self.base = json.loads(docker(*self.common, *source_args, "config", "--format", "json"))
        self.project = self.base["name"]
        self.service = args.service
        for name in (self.project, self.service):
            if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*", name):
                raise DeployError("Invalid project or service name.")
        if self.service not in self.base.get("services", {}):
            raise DeployError("Service not found in Compose: " + self.service)
        self.common += ["--project-name", self.project]
        self.state_dir = self.directory / ".sxuoj-deploy" / self.project / self.service
        self.current = self.state_dir / "current.compose.json"
        self.previous = self.state_dir / "previous.compose.json"
        self.metadata = self.state_dir / "release.json"
        self.previous_metadata = self.state_dir / "previous.release.json"
        self.origin = self.state_dir / "origin.json"

    def compose(self, path, *args):
        return docker(*self.common, "-f", str(path), *args)

    def running(self):
        ids = docker("ps", "-a", "--filter", "label=com.docker.compose.project=" + self.project,
                     "--filter", "label=com.docker.compose.service=" + self.service,
                     "--filter", "label=com.docker.compose.oneoff=False", "--format", "{{.ID}}").split()
        if len(ids) != 1:
            raise DeployError("Expected one existing backend container. Check --project-name and --service.")
        return inspect("container", ids[0])

    def verify_origin(self, container):
        labels = container.get("Config", {}).get("Labels", {}) or {}
        recorded = labels.get("com.docker.compose.project.config_files", "")
        actual = [str(Path(path).resolve()) for path in recorded.split(",") if path]
        allowed = [str(path) for path in self.files]
        managed = {str(self.state_dir / name) for name in
                   ("candidate.compose.json", "recovery.compose.json", "current.compose.json",
                    "previous.compose.json")}
        if actual != allowed and not (len(actual) == 1 and actual[0] in managed):
            raise DeployError("Compose files differ from the running container. Pass every original -f file.")
        if len(actual) == 1 and actual[0] in managed:
            if not self.origin.exists():
                raise DeployError("Managed release origin is missing; inspect its recovery snapshot before continuing.")
            saved_origin = json.loads(self.origin.read_text(encoding="utf-8"))
            if saved_origin.get("compose_files") != allowed:
                raise DeployError("Original Compose file list/order differs from the managed release.")
        if self.args.action == "update" and container.get("State", {}).get("Status") != "running":
            raise DeployError("The current backend is not running; restore it before releasing.")

    def read_runtime(self, path):
        model = {"services": {"env": {"image": "scratch", "env_file": [str(path)]}}}
        parsed = json.loads(docker(*self.common, "-f", "-", "config", "--format", "json",
                                   stdin=json.dumps(model)))
        return parsed["services"]["env"].get("environment", {})

    def runtime_file(self):
        if self.args.clear_runtime_env:
            return None
        if self.args.runtime_env:
            return Path(self.args.runtime_env).resolve(strict=True)
        if self.metadata.exists():
            stored = json.loads(self.metadata.read_text(encoding="utf-8")).get("runtime_env")
            if stored:
                return Path(stored).resolve(strict=True)
        return None

    def old_model(self, container):
        model = json.loads(self.compose(self.current, "config", "--format", "json")) if self.current.exists() else copy.deepcopy(self.base)
        service = model["services"][self.service]
        service["image"] = container["Image"]  # actual old ID, even if :latest was repointed
        service["pull_policy"] = "never"
        service.pop("build", None)
        # Unlike `compose config`, Docker inspect returns unescaped runtime values.
        service["environment"] = literal_compose(env_map(container["Config"].get("Env")))
        service["entrypoint"] = literal_compose(container["Config"].get("Entrypoint") or [])
        service["command"] = literal_compose(container["Config"].get("Cmd") or [])
        return model

    def wait_ready(self, require_health=True):
        deadline = time.monotonic() + self.args.timeout
        while time.monotonic() < deadline:
            container = self.running()
            state = container.get("State", {})
            status = state.get("Status")
            health = state.get("Health", {}).get("Status")
            if status == "running" and health == "healthy":
                return
            if status == "running" and not health and not require_health:
                time.sleep(3)
                if self.running().get("State", {}).get("Status") == "running":
                    return
            if status in ("exited", "dead") or health == "unhealthy":
                raise DeployError("Backend failed its startup health check.")
            time.sleep(2)
        raise DeployError("Timed out waiting for the backend health check.")

    def up(self, snapshot):
        self.compose(snapshot, "up", "-d", "--no-deps", "--no-build", "--pull", "never", self.service)

    def apply(self, candidate, old, runtime_path, image_name, old_has_health):
        candidate_path = self.state_dir / "candidate.compose.json"
        recovery_path = self.state_dir / "recovery.compose.json"
        # `compose config --format json` already escapes literal dollars. Re-escaping
        # that model would silently change passwords containing '$' on every release.
        write_private(candidate_path, candidate)
        write_private(recovery_path, old)
        self.compose(candidate_path, "config", "--quiet")
        write_private(self.origin, {"compose_files": [str(path) for path in self.files]})
        # Persist recovery before the first container mutation, including interrupted releases.
        print("Recreating only %s/%s; waiting for health..." % (self.project, self.service), flush=True)
        try:
            self.up(candidate_path)
            probe = candidate["services"][self.service].get("healthcheck", {})
            self.wait_ready(require_health=bool(probe) and not probe.get("disable", False))
        except (DeployError, KeyboardInterrupt) as error:
            print("Release failed; restoring the previous image and environment...", file=sys.stderr, flush=True)
            try:
                self.up(recovery_path)
                self.wait_ready(require_health=old_has_health)
            except (DeployError, KeyboardInterrupt) as rollback_error:
                raise DeployError("Automatic rollback also failed. Recovery snapshot: " + str(recovery_path)) from rollback_error
            raise DeployError("Release failed; the previous backend has been restored.") from error
        write_private(self.previous, old)
        write_private(self.current, candidate)
        prior_metadata = json.loads(self.metadata.read_text(encoding="utf-8")) if self.metadata.exists() else {}
        write_private(self.previous_metadata, prior_metadata)
        write_private(self.metadata, {
            "project": self.project, "service": self.service, "requested_image": image_name,
            "image_id": candidate["services"][self.service]["image"],
            "runtime_env": str(runtime_path) if runtime_path else None,
            "compose_files": [str(path) for path in self.files],
        })
        print("Backend is healthy. Release snapshot: " + str(self.current))

    def execute(self):
        container = self.running()
        self.verify_origin(container)
        if self.args.action == "status":
            print(json.dumps({"project": self.project, "service": self.service,
                              "image_id": container["Image"],
                              "status": container["State"]["Status"],
                              "health": container["State"].get("Health", {}).get("Status", "not configured"),
                              "rollback_available": self.previous.exists()}, indent=2))
            return
        if self.args.action == "rollback" and not self.previous.exists():
            raise DeployError("No previous release snapshot is available.")
        runtime_path = self.runtime_file() if self.args.action == "update" else None
        if self.args.action == "update":
            candidate = copy.deepcopy(self.base)
            service = candidate["services"][self.service]
            service.setdefault("environment", {}).update(self.read_runtime(runtime_path) if runtime_path else {})
            validate_secrets(service["environment"])
            if not self.args.image or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_./:@-]*", self.args.image):
                raise DeployError("Provide a valid --image reference.")
        if self.args.dry_run:
            print(json.dumps({"action": self.args.action, "project": self.project, "service": self.service,
                              "image": self.args.image, "runtime_env": str(runtime_path) if runtime_path else None,
                              "effect": "only backend recreated; original Compose files and dependencies retained"}, indent=2))
            return
        with release_lock(self.state_dir):
            # Recheck after acquiring the lock so a concurrent release cannot change the baseline.
            container = self.running()
            self.verify_origin(container)
            old = self.old_model(container)
            if self.args.action == "update":
                if not self.args.no_pull:
                    print("Pulling " + self.args.image, flush=True)
                    docker("pull", self.args.image)
                image = inspect("image", self.args.image)
                if (image["Config"].get("Labels") or {}).get("io.sxuoj.component") != "backend":
                    raise DeployError("Image is not an SXUOJ backend produced by this pipeline.")
                service["image"] = image["Id"]
                service["pull_policy"] = "never"
                service.pop("build", None)
                # Override legacy health probes that target authenticated /actuator/health.
                service["healthcheck"] = {"test": ["CMD", "/usr/local/bin/hoj-healthcheck"],
                                          "interval": "10s", "timeout": "5s", "start_period": "30s", "retries": 18}
                requested = self.args.image
            else:
                candidate = json.loads(self.compose(self.previous, "config", "--format", "json"))
                inspect("image", candidate["services"][self.service]["image"])
                requested = candidate["services"][self.service]["image"]
                prior_metadata = json.loads(self.previous_metadata.read_text(encoding="utf-8")) if self.previous_metadata.exists() else {}
                runtime_path = prior_metadata.get("runtime_env")
            self.apply(candidate, old, runtime_path, requested, bool(container["State"].get("Health")))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("update", "rollback", "status"))
    parser.add_argument("-f", "--compose", action="append", required=True, help="Original Compose file; repeat in original order")
    parser.add_argument("--env-file", action="append", default=[], help="Original Compose interpolation .env; repeat if needed")
    parser.add_argument("-p", "--project-name", help="Existing Compose project name when started with -p")
    parser.add_argument("--service", default="hoj-backend")
    parser.add_argument("--image", help="Published backend image tag or digest")
    parser.add_argument("--no-pull", action="store_true", help="Use an already loaded local image (offline deployment)")
    env_group = parser.add_mutually_exclusive_group()
    env_group.add_argument("--runtime-env", help="Additional container environment file; overrides original environment")
    env_group.add_argument("--clear-runtime-env", action="store_true", help="Stop importing the previously selected runtime file")
    parser.add_argument("--timeout", type=int, default=240)
    parser.add_argument("--dry-run", action="store_true", help="Read-only configuration check; no pull, file writes or recreation")
    args = parser.parse_args(argv)
    if args.timeout < 10:
        parser.error("--timeout must be at least 10 seconds")
    try:
        Deployment(args).execute()
    except (DeployError, OSError, ValueError, KeyError) as error:
        print("Deployment stopped: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
