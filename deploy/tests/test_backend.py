"""Offline release regressions: Docker actions are mocked; config-only CLI checks need no daemon."""

import base64
import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "backend.py"
SPEC = importlib.util.spec_from_file_location("sxuoj_backend_deploy", SCRIPT)
backend = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backend)


def credentials():
    return {
        "JWT_TOKEN_SECRET": base64.b64encode(bytes(range(64))).decode("ascii"),
        "JUDGE_TOKEN": "unit-test-judge-token-0123456789-abcdef",
        "NACOS_PASSWORD": "test-only-strong-nacos-password",
        "NACOS_ENABLED": "true",
    }


def unescape_literals(value):
    """Model Compose's $$ escape for fake snapshots; a real CLI test checks this contract."""
    if isinstance(value, str):
        return value.replace("$$", "$")
    if isinstance(value, list):
        return [unescape_literals(item) for item in value]
    if isinstance(value, dict):
        return {key: unescape_literals(item) for key, item in value.items()}
    return value


class FakeDocker:
    """Reject every unexpected Docker action so tests cannot silently widen deploy scope."""

    def __init__(self, compose_files):
        self.compose_files = [str(path.resolve()) for path in compose_files]
        self.calls = []
        self.applied = []
        self.runtime = {}
        self.up_failures = []
        self.health_results = []
        self.ids = "old-container"
        self.model = {
            "name": "existing-prod",
            "services": {
                "hoj-backend": {
                    "image": "registry.example/hoj:latest",
                    "build": {"context": "legacy-backend"},
                    "environment": {**credentials(), "KEEP": "from-compose", "OVERRIDE": "original"},
                    "ports": [{"target": 6688, "published": "6688", "protocol": "tcp"}],
                    "volumes": [{"type": "volume", "source": "uploads", "target": "/hoj/upload"}],
                    "networks": {"legacy": {"ipv4_address": "172.20.0.6"}},
                    "depends_on": {"mysql": {"condition": "service_started", "required": True}},
                    "healthcheck": {"test": ["CMD", "legacy-health"]},
                },
                "mysql": {"image": "mysql:8", "volumes": [{"type": "volume", "source": "db", "target": "/var/lib/mysql"}]},
            },
            "volumes": {"uploads": {"name": "existing_uploads"}, "db": {"name": "existing_db", "external": True}},
            "networks": {"legacy": {"name": "existing_network", "external": True}},
        }
        self.container = {
            "Id": "old-container",
            "Image": "sha256:actual-old-image",
            "Config": {
                "Env": [f"{key}={value}" for key, value in {**credentials(), "RUNNING_ONLY": "actual-old=$value", "OVERRIDE": "old-effective"}.items()],
                "Entrypoint": ["/old-entrypoint"],
                "Cmd": ["java", "-Dliteral=$SECRET", "-jar", "old.jar"],
                "Labels": {
                    "com.docker.compose.project": "existing-prod",
                    "com.docker.compose.service": "hoj-backend",
                    "com.docker.compose.project.config_files": ",".join(self.compose_files),
                },
            },
            "State": {"Status": "running", "Health": {"Status": "healthy"}},
        }
        self.new_image = {"Id": "sha256:new-immutable-image", "Config": {"Labels": {"io.sxuoj.component": "backend"}}}

    def read_model(self, path):
        return unescape_literals(json.loads(Path(path).read_text(encoding="utf-8")))

    def __call__(self, *args, stdin=None):
        self.calls.append((args, stdin))
        if args[0] == "ps":
            filters = [args[index + 1] for index, item in enumerate(args) if item == "--filter"]
            if "label=com.docker.compose.project=existing-prod" not in filters:
                return ""
            if "label=com.docker.compose.service=hoj-backend" not in filters:
                return ""
            return self.ids
        if args[:2] == ("container", "inspect"):
            return json.dumps([self.container])
        if args[0] == "pull":
            return ""
        if args[:2] == ("image", "inspect"):
            image = self.new_image if args[2] == "registry.example/sxuoj:release" else {"Id": args[2], "Config": {}}
            return json.dumps([image])
        if args[0] != "compose":
            raise AssertionError("Unexpected Docker action: " + repr(args))
        paths = [args[index + 1] for index, item in enumerate(args) if item == "-f"]
        if "config" in args:
            if "--quiet" in args:
                self.read_model(paths[-1])
                return ""
            if paths == ["-"]:
                supplied = json.loads(stdin)
                assert supplied["services"]["env"]["env_file"]
                return json.dumps({"services": {"env": {"environment": backend.literal_compose(self.runtime)}}})
            if all(path in self.compose_files for path in paths):
                result = copy.deepcopy(self.model)
                if "--project-name" in args:
                    result["name"] = args[args.index("--project-name") + 1]
                return json.dumps(backend.literal_compose(result))
            return Path(paths[-1]).read_text(encoding="utf-8")
        if "up" in args:
            model = self.read_model(paths[-1])
            self.applied.append(model)
            failure = self.up_failures.pop(0) if self.up_failures else None
            if failure:
                raise failure
            service = model["services"]["hoj-backend"]
            self.container["Image"] = service["image"]
            self.container["Config"]["Env"] = [f"{key}={value}" for key, value in service.get("environment", {}).items()]
            self.container["Config"]["Labels"]["com.docker.compose.project.config_files"] = paths[-1]
            for source, target in (("entrypoint", "Entrypoint"), ("command", "Cmd")):
                self.container["Config"][target] = service.get(source)
            health = self.health_results.pop(0) if self.health_results else "healthy"
            self.container["State"] = {"Status": "running", "Health": {"Status": health}}
            return ""
        raise AssertionError("Unexpected Compose action: " + repr(args))


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.compose = self.directory / "docker-compose.yml"
        self.compose.write_text("# Existing operator-owned Compose file\n", encoding="utf-8")
        self.fake = FakeDocker([self.compose])
        self.patch = mock.patch.object(backend, "docker", side_effect=self.fake)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.state = self.directory / ".sxuoj-deploy" / "existing-prod" / "hoj-backend"
        self.output = io.StringIO()
        self.errors = io.StringIO()

    def run_release(self, action="update", *options):
        arguments = [action, "-f", str(self.compose), "--timeout", "10"]
        if action == "update":
            arguments += ["--image", "registry.example/sxuoj:release"]
        with contextlib.redirect_stdout(self.output), contextlib.redirect_stderr(self.errors):
            return backend.main(arguments + list(options))

    def assert_no_mutations(self):
        self.assertFalse(self.fake.applied)
        self.assertFalse(any(args[0] == "pull" for args, _ in self.fake.calls))
        self.assertFalse(self.state.exists())

    def test_update_recreates_only_target_preserving_operator_compose_and_topology(self):
        original_bytes = self.compose.read_bytes()
        original_model = copy.deepcopy(self.fake.model)
        self.assertEqual(0, self.run_release())
        self.assertEqual(original_bytes, self.compose.read_bytes())
        self.assertEqual(1, len(self.fake.applied))
        applied = self.fake.applied[0]
        for key in ("volumes", "networks"):
            self.assertEqual(original_model[key], applied[key])
        self.assertEqual(original_model["services"]["mysql"], applied["services"]["mysql"])
        for key in ("volumes", "networks", "depends_on", "ports"):
            self.assertEqual(original_model["services"]["hoj-backend"][key], applied["services"]["hoj-backend"][key])
        target = applied["services"]["hoj-backend"]
        self.assertEqual("sha256:new-immutable-image", target["image"])
        self.assertNotIn("build", target)
        up = next(args for args, _ in self.fake.calls if "up" in args)
        self.assertEqual(("up", "-d", "--no-deps", "--no-build", "--pull", "never", "hoj-backend"), up[up.index("up"):])
        self.assertEqual(["CMD", "/usr/local/bin/hoj-healthcheck"], target["healthcheck"]["test"])

    def test_explicit_rollback_uses_running_image_id_and_actual_environment(self):
        old = copy.deepcopy(self.fake.container)
        self.assertEqual(0, self.run_release())
        self.assertEqual(0, self.run_release("rollback"))
        restored = self.fake.applied[-1]["services"]["hoj-backend"]
        self.assertEqual(old["Image"], restored["image"])
        self.assertEqual(backend.env_map(old["Config"]["Env"]), restored["environment"])
        self.assertEqual(old["Config"]["Entrypoint"], restored["entrypoint"])
        self.assertEqual(old["Config"]["Cmd"], restored["command"])
        self.assertEqual(1, sum(args[0] == "pull" for args, _ in self.fake.calls))

    def test_unhealthy_release_automatically_restores_without_advancing_snapshot(self):
        self.fake.health_results = ["unhealthy", "healthy"]
        self.assertEqual(1, self.run_release())
        self.assertEqual(2, len(self.fake.applied))
        self.assertEqual("sha256:actual-old-image", self.fake.container["Image"])
        self.assertEqual("actual-old=$value", backend.env_map(self.fake.container["Config"]["Env"])["RUNNING_ONLY"])
        self.assertFalse((self.state / "current.compose.json").exists())
        self.assertTrue((self.state / "recovery.compose.json").exists())
        self.assertIn("previous backend has been restored", self.errors.getvalue())
        self.assertFalse((self.state / "lock").exists())

    def test_up_failure_and_interrupt_restore_the_previous_container(self):
        for failure in (backend.DeployError("recreate failed"), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__):
                self.fake.up_failures = [failure, None]
                self.assertEqual(1, self.run_release())
                self.assertEqual("sha256:actual-old-image", self.fake.container["Image"])
                self.assertFalse((self.state / "current.compose.json").exists())

    def test_failed_automatic_rollback_reports_recovery_snapshot(self):
        self.fake.health_results = ["unhealthy", "unhealthy"]
        self.assertEqual(1, self.run_release())
        self.assertIn("Automatic rollback also failed", self.errors.getvalue())
        self.assertIn("recovery.compose.json", self.errors.getvalue())
        self.assertFalse((self.state / "current.compose.json").exists())

    def test_weak_credentials_stop_before_pull_write_or_recreate_without_leaking(self):
        for field, value in (("JWT_TOKEN_SECRET", "secret-invalid-base64!"),
                             ("JWT_TOKEN_SECRET", base64.b64encode(b"short-secret").decode("ascii")),
                             ("JUDGE_TOKEN", "judge-private"),
                             ("NACOS_PASSWORD", "nac-private"),
                             ("NACOS_ENABLED", "false")):
            with self.subTest(field=field, value=value):
                self.fake.model["services"]["hoj-backend"]["environment"] = {**credentials(), field: value}
                self.assertEqual(1, self.run_release())
                self.assert_no_mutations()
                self.assertNotIn(value, self.output.getvalue() + self.errors.getvalue())

    def test_runtime_env_is_parsed_by_compose_and_overrides_existing_environment(self):
        runtime = self.directory / "backend.env"
        runtime.write_text("# Parsed by Compose, never by a hand-written dotenv parser\n", encoding="utf-8")
        interpolation = self.directory / "compose.env"
        interpolation.write_text("SOME_VALUE=test\n", encoding="utf-8")
        self.fake.runtime = {"OVERRIDE": "runtime$#value", "EXTRA": "line1\nline2", "NACOS_PASSWORD": "runtime-private-password"}
        self.assertEqual(0, self.run_release("update", "--runtime-env", str(runtime), "--env-file", str(interpolation)))
        applied_env = self.fake.applied[0]["services"]["hoj-backend"]["environment"]
        self.assertEqual("from-compose", applied_env["KEEP"])
        self.assertEqual("runtime$#value", applied_env["OVERRIDE"])
        self.assertEqual("line1\nline2", applied_env["EXTRA"])
        parse_call = next((args, data) for args, data in self.fake.calls if data)
        self.assertIn("--env-file", parse_call[0])
        self.assertIn(str(interpolation.resolve()), parse_call[0])
        self.assertEqual([str(runtime.resolve())], json.loads(parse_call[1])["services"]["env"]["env_file"])
        self.assertNotIn("runtime-private-password", self.output.getvalue() + self.errors.getvalue())

    def test_successful_update_remembers_runtime_env_until_explicitly_cleared(self):
        runtime = self.directory / "backend.env"
        runtime.write_text("OVERRIDE=runtime\n", encoding="utf-8")
        self.fake.runtime = {"OVERRIDE": "runtime"}
        self.assertEqual(0, self.run_release("update", "--runtime-env", str(runtime)))
        self.assertEqual(0, self.run_release())
        self.assertEqual("runtime", self.fake.applied[-1]["services"]["hoj-backend"]["environment"]["OVERRIDE"])
        self.assertEqual(0, self.run_release("update", "--clear-runtime-env"))
        self.assertEqual("original", self.fake.applied[-1]["services"]["hoj-backend"]["environment"]["OVERRIDE"])
        self.assertIsNone(json.loads((self.state / "release.json").read_text(encoding="utf-8"))["runtime_env"])

    def test_dry_run_does_not_write_pull_or_recreate(self):
        runtime = self.directory / "backend.env"
        runtime.write_text("OVERRIDE=test\n", encoding="utf-8")
        before = sorted(path.relative_to(self.directory).as_posix() for path in self.directory.rglob("*"))
        self.assertEqual(0, self.run_release("update", "--runtime-env", str(runtime), "--dry-run"))
        self.assert_no_mutations()
        self.assertEqual(before, sorted(path.relative_to(self.directory).as_posix() for path in self.directory.rglob("*")))

    def test_offline_update_uses_loaded_image_without_pulling(self):
        self.assertEqual(0, self.run_release("update", "--no-pull"))
        self.assertEqual("sha256:new-immutable-image", self.fake.container["Image"])
        self.assertFalse(any(args[0] == "pull" for args, _ in self.fake.calls))

    def test_rollback_restores_previous_runtime_file_selection(self):
        first = self.directory / "first.env"
        second = self.directory / "second.env"
        first.write_text("OVERRIDE=first\n", encoding="utf-8")
        second.write_text("OVERRIDE=second\n", encoding="utf-8")
        self.fake.runtime = {"OVERRIDE": "first$literal"}
        self.assertEqual(0, self.run_release("update", "--runtime-env", str(first)))
        self.fake.runtime = {"OVERRIDE": "second$literal"}
        self.assertEqual(0, self.run_release("update", "--runtime-env", str(second)))
        self.assertEqual(0, self.run_release("rollback"))
        metadata = json.loads((self.state / "release.json").read_text(encoding="utf-8"))
        self.assertEqual(str(first.resolve()), metadata["runtime_env"])
        self.assertEqual("first$literal", self.fake.applied[-1]["services"]["hoj-backend"]["environment"]["OVERRIDE"])

    def test_manual_rollback_can_recover_backend_that_has_stopped(self):
        self.assertEqual(0, self.run_release())
        self.fake.container["State"] = {"Status": "exited"}
        self.assertEqual(0, self.run_release("rollback"))
        self.assertEqual("sha256:actual-old-image", self.fake.container["Image"])

    def test_dollar_credentials_stay_stable_across_releases_and_rollback(self):
        raw = {"LITERAL": "$NAME", "BRACES": "${NAME}", "DOUBLED": "$$NAME", "MIXED": "$5=a$#b"}
        self.fake.model["services"]["hoj-backend"]["environment"].update(raw)
        self.assertEqual(0, self.run_release())
        self.assertEqual(0, self.run_release())
        self.assertEqual(0, self.run_release("rollback"))
        for applied in self.fake.applied:
            env = applied["services"]["hoj-backend"]["environment"]
            for key, value in raw.items():
                self.assertEqual(value, env[key])

    def test_missing_or_ambiguous_backend_is_refused_before_mutation(self):
        for ids in ("", "container1 container2"):
            with self.subTest(ids=ids):
                self.fake.ids = ids
                self.assertEqual(1, self.run_release())
                self.assert_no_mutations()

    def test_wrong_project_service_or_compose_is_refused_before_mutation(self):
        self.assertEqual(1, self.run_release("update", "--project-name", "different-project"))
        self.assert_no_mutations()
        self.assertEqual(1, self.run_release("update", "--service", "not-a-service"))
        self.assert_no_mutations()
        self.fake.container["Config"]["Labels"]["com.docker.compose.project.config_files"] = str(self.directory / "wrong.yml")
        self.assertEqual(1, self.run_release())
        self.assert_no_mutations()

    def test_original_compose_override_order_must_match_running_container(self):
        override = self.directory / "override.yml"
        override.write_text("# second original Compose file\n", encoding="utf-8")
        self.fake.compose_files.append(str(override.resolve()))
        self.fake.container["Config"]["Labels"]["com.docker.compose.project.config_files"] = ",".join(reversed(self.fake.compose_files))
        self.assertEqual(1, self.run_release("update", "-f", str(override)))
        self.assert_no_mutations()

    def test_managed_container_still_rejects_a_different_original_compose_file(self):
        self.assertEqual(0, self.run_release())
        wrong = self.directory / "other-stack.yml"
        wrong.write_text("# Unrelated Compose file in the same directory\n", encoding="utf-8")
        self.fake.compose_files.append(str(wrong.resolve()))
        self.compose = wrong
        previous_calls = len(self.fake.calls)
        self.assertEqual(1, self.run_release())
        self.assertEqual(1, len(self.fake.applied))
        self.assertFalse(any(args[0] == "pull" for args, _ in self.fake.calls[previous_calls:]))

    def test_failed_first_release_recovery_keeps_original_compose_identity(self):
        self.fake.health_results = ["unhealthy", "healthy"]
        self.assertEqual(1, self.run_release())
        wrong = self.directory / "other-stack.yml"
        wrong.write_text("# Unrelated Compose file\n", encoding="utf-8")
        self.fake.compose_files.append(str(wrong.resolve()))
        self.compose = wrong
        previous_calls = len(self.fake.calls)
        self.assertEqual(1, self.run_release())
        self.assertEqual(2, len(self.fake.applied))
        self.assertFalse(any(args[0] == "pull" for args, _ in self.fake.calls[previous_calls:]))

    def test_first_failure_can_be_retried_then_rolled_back_with_same_original_files(self):
        self.fake.health_results = ["unhealthy", "healthy"]
        self.assertEqual(1, self.run_release())
        self.assertEqual(0, self.run_release())
        self.assertEqual(0, self.run_release("rollback"))
        self.assertEqual("sha256:actual-old-image", self.fake.container["Image"])
        self.assertEqual("actual-old=$value", backend.env_map(self.fake.container["Config"]["Env"])["RUNNING_ONLY"])

    def test_stopped_backend_or_missing_rollback_is_refused(self):
        self.fake.container["State"]["Status"] = "exited"
        self.assertEqual(1, self.run_release())
        self.assert_no_mutations()
        self.fake.container["State"]["Status"] = "running"
        self.assertEqual(1, self.run_release("rollback"))
        self.assert_no_mutations()

    def test_non_backend_image_is_not_applied(self):
        self.fake.new_image["Config"]["Labels"] = {}
        self.assertEqual(1, self.run_release())
        self.assertFalse(self.fake.applied)
        self.assertFalse((self.state / "current.compose.json").exists())

    def test_state_and_snapshot_permissions_are_private(self):
        original_chmod = os.chmod
        with mock.patch.object(backend.os, "chmod", wraps=original_chmod) as chmod:
            self.assertEqual(0, self.run_release())
        calls = [(Path(call.args[0]), call.args[1]) for call in chmod.call_args_list]
        self.assertIn((self.state, 0o700), calls)
        for path in self.state.glob("*.json"):
            self.assertIn((path.with_name(path.name + ".tmp"), 0o600), calls)
            if os.name != "nt":
                self.assertEqual(0o600, path.stat().st_mode & 0o777)
        if os.name != "nt":
            self.assertEqual(0o700, self.state.stat().st_mode & 0o777)

    def test_status_is_read_only_and_never_discloses_environment(self):
        self.assertEqual(0, self.run_release("status"))
        self.assert_no_mutations()
        result = json.loads(self.output.getvalue())
        self.assertEqual("sha256:actual-old-image", result["image_id"])
        for value in credentials().values():
            self.assertNotIn(value, self.output.getvalue())


class DockerBoundaryTests(unittest.TestCase):
    def require_compose_cli(self):
        executable = shutil.which("docker")
        if not executable:
            self.skipTest("Docker CLI unavailable; no container or daemon is required for this check")
        version = subprocess.run([executable, "compose", "version", "--short"], capture_output=True, text=True)
        if version.returncode:
            self.skipTest("Compose v2 unavailable")
        return executable

    def test_docker_diagnostics_do_not_disclose_interpolated_credentials(self):
        result = subprocess.CompletedProcess([], 1, stdout="MYSQL_PASSWORD=private-secret", stderr="private-secret invalid")
        with mock.patch.object(backend.subprocess, "run", return_value=result):
            with self.assertRaises(backend.DeployError) as raised:
                backend.docker("compose", "config", "--format", "json")
        self.assertNotIn("private-secret", str(raised.exception))

    def test_literal_snapshot_preserves_dollar_values_through_real_compose_config(self):
        executable = self.require_compose_cli()
        values = {"PLAIN": "$SECRET", "BRACES": "${SECRET}", "DOUBLE": "$$SECRET", "MIXED": "a$#b=$5", "MULTILINE": "one\ntwo$SECRET"}
        command = ["sh", "-c", "printf '%s' '$SECRET ${SECRET} $$SECRET'"]
        model = {"name": "sxuoj-config-only-test", "services": {"backend": {"image": "scratch", "environment": values, "command": command}}}
        payload = json.dumps(backend.literal_compose(model))
        for iteration in range(3):
            with self.subTest(snapshot_roundtrip=iteration):
                result = subprocess.run([executable, "compose", "-f", "-", "config", "--format", "json"],
                                        input=payload, capture_output=True, text=True, encoding="utf-8",
                                        env={**os.environ, "SECRET": "must-not-interpolate"})
                self.assertEqual(0, result.returncode, result.stderr)
                parsed = json.loads(result.stdout)
                # Compose config is a reusable escaped model, unlike Docker inspect's raw values.
                self.assertEqual(backend.literal_compose(values), parsed["services"]["backend"]["environment"])
                self.assertEqual(backend.literal_compose(command), parsed["services"]["backend"]["command"])
                with tempfile.TemporaryDirectory() as directory:
                    snapshot = Path(directory) / "snapshot.compose.json"
                    backend.write_private(snapshot, parsed)
                    payload = snapshot.read_text(encoding="utf-8")

    def test_runtime_file_quotes_and_env_file_precedence_use_real_compose_parser(self):
        self.require_compose_cli()
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            first = directory / "first.env"
            second = directory / "second.env"
            runtime = directory / "runtime.env"
            first.write_text("SXUOJ_CONFIG_TEST_ONLY=first\n", encoding="utf-8")
            second.write_text("SXUOJ_CONFIG_TEST_ONLY=second\n", encoding="utf-8")
            runtime.write_bytes(b"EXPANDED=${SXUOJ_CONFIG_TEST_ONLY}\nLITERAL='keep$SECRET # hash'\nMULTILINE='line1\nline2'\nEMPTY=\n")
            deployment = object.__new__(backend.Deployment)
            deployment.common = ["compose", "--project-directory", str(directory), "--env-file", str(first), "--env-file", str(second)]
            with mock.patch.dict(os.environ):
                os.environ.pop("SXUOJ_CONFIG_TEST_ONLY", None)
                env = deployment.read_runtime(runtime)
            self.assertEqual({"EXPANDED": "second", "LITERAL": "keep$$SECRET # hash", "MULTILINE": "line1\nline2", "EMPTY": ""}, env)


if __name__ == "__main__":
    unittest.main()
