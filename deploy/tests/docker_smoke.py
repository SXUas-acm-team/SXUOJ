"""Opt-in disposable Compose test; never uses production credentials or services.

Run after building sxuoj-backend:local-check:
    python3 deploy/tests/docker_smoke.py
"""
import base64
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid


def run(*args, stdin=None, success=True):
    result = subprocess.run(list(args), input=stdin, text=True, encoding="utf-8", capture_output=True)
    if success and result.returncode:
        raise RuntimeError("Command failed: " + " ".join(args[:3]) + "\n" + result.stderr)
    return result


def docker(*args, **kwargs):
    return run("docker", *args, **kwargs).stdout.strip()


def main():
    project = "sxuoj-smoke-" + uuid.uuid4().hex[:10]
    script = Path(__file__).resolve().parents[1] / "backend.py"
    tags = [project + ":" + tag for tag in ("old", "new", "broken")]
    with tempfile.TemporaryDirectory(prefix=project) as directory:
        root = Path(directory)
        compose = root / "compose.json"
        runtime = root / "backend.env"
        env = {"JWT_TOKEN_SECRET": base64.b64encode(b"smoke-only!" * 8).decode(),
               "JUDGE_TOKEN": "smoke-only-judge-token-do-not-use-in-production",
               "NACOS_PASSWORD": "smoke-only-nacos-password", "NACOS_ENABLED": "true",
               "LITERAL": "base-value"}
        runtime.write_text("LITERAL='smoke$value#literal'\n", encoding="utf-8")
        model = {"name": project, "services": {
            "hoj-backend": {"image": tags[0], "environment": env,
                            "volumes": ["uploads:/hoj/file"],
                            "healthcheck": {"test": ["CMD", "/usr/local/bin/hoj-healthcheck"],
                                            "interval": "1s", "timeout": "1s", "retries": 2}},
            "dependency": {"image": tags[0]}}, "volumes": {"uploads": {}}}
        compose.write_text(json.dumps(model), encoding="utf-8")
        compose_args = ["compose", "-p", project, "-f", str(compose)]
        release_args = [str(script), "-f", str(compose), "-p", project, "--timeout", "25"]
        try:
            for tag, code in zip(tags, (0, 0, 1)):
                # Process and health fixtures inherit the real image but never start Java.
                source = ("FROM sxuoj-backend:local-check\n"
                          "RUN printf '#!/bin/sh\\nexit %s\\n' > /usr/local/bin/hoj-healthcheck "
                          "&& chmod +x /usr/local/bin/hoj-healthcheck\n"
                          "ENTRYPOINT [\"/bin/sh\",\"-c\",\"exec tail -f /dev/null\"]\n"
                          "LABEL smoke.version=\"%s\"\n" % (code, tag))
                docker("build", "-t", tag, "-", stdin=source)
            docker(*compose_args, "up", "-d", "--wait", "--wait-timeout", "30")
            old_id = docker(*compose_args, "ps", "-q", "hoj-backend")
            dependency_id = docker(*compose_args, "ps", "-q", "dependency")
            old_image = json.loads(docker("container", "inspect", old_id))[0]["Image"]
            docker("exec", old_id, "sh", "-c", "printf preserved > /hoj/file/smoke-marker")

            def release(action, *options, success=True):
                # argparse accepts options either side of its action positional.
                return run(sys.executable, "-X", "utf8", *release_args, action, *options, success=success)

            release("update", "--image", tags[1], "--no-pull", "--runtime-env", str(runtime))
            new_id = docker(*compose_args, "ps", "-q", "hoj-backend")
            new_container = json.loads(docker("container", "inspect", new_id))[0]
            assert new_id != old_id
            assert "LITERAL=smoke$value#literal" in new_container["Config"]["Env"]
            assert docker("exec", new_id, "cat", "/hoj/file/smoke-marker") == "preserved"
            assert docker(*compose_args, "ps", "-q", "dependency") == dependency_id

            failed = release("update", "--image", tags[2], "--no-pull", success=False)
            assert failed.returncode != 0
            assert "previous backend has been restored" in failed.stderr, failed.stderr
            restored_id = docker(*compose_args, "ps", "-q", "hoj-backend")
            assert json.loads(docker("container", "inspect", restored_id))[0]["Image"] == new_container["Image"]

            release("rollback")
            reverted_id = docker(*compose_args, "ps", "-q", "hoj-backend")
            assert json.loads(docker("container", "inspect", reverted_id))[0]["Image"] == old_image
            assert docker("exec", reverted_id, "cat", "/hoj/file/smoke-marker") == "preserved"
            assert docker(*compose_args, "ps", "-q", "dependency") == dependency_id
            print("Docker smoke passed: update, literal environment, failed-update recovery, rollback, volumes and dependency preserved.")
        finally:
            # The unique project owns only this test's containers, network and named volume.
            docker(*compose_args, "down", "--volumes", "--remove-orphans", success=False)
            for tag in tags:
                docker("image", "rm", tag, success=False)


if __name__ == "__main__":
    main()
