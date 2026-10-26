"""Opt-in local Docker checks for image replacement, legacy configuration and TLS."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def docker(*args, stdin=None, check=True):
    result = subprocess.run(["docker", *args], input=stdin, text=True, encoding="utf-8",
                            capture_output=True, timeout=120)
    if check and result.returncode:
        raise RuntimeError("isolated Docker operation failed: " + " ".join(args[:2]))
    return result


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="sxuoj-frontend:contest-security-20261020")
    parser.add_argument("--backend-image", default="sxuoj-backend:contest-security-20261020")
    parser.add_argument("--openssl", default="openssl")
    args = parser.parse_args()
    name = "sxuoj-frontend-smoke-" + uuid.uuid4().hex[:10]
    assertions, containers = [], []
    root = Path(tempfile.mkdtemp(prefix=name + "-")).resolve()
    # Validate the recursive cleanup target before any files are created.
    if root.parent != Path(tempfile.gettempdir()).resolve() or not root.name.startswith(name):
        raise RuntimeError("temporary smoke directory is outside its expected root")
    network_created = False

    def passed(label, condition):
        if not condition:
            raise AssertionError(label)
        assertions.append(label)

    def container(suffix, env=None, mounts=None, tls=False, upstream=False):
        command = ["run", "-d", "--name", name + "-" + suffix, "--network", name,
                   "--label", "io.sxuoj.test=" + name]
        if upstream:
            command += ["--network-alias", "upstream", "--entrypoint", "nginx"]
        else:
            command += ["-p", "127.0.0.1::80"]
            if tls:
                command += ["-p", "127.0.0.1::443"]
        for key, value in (env or {}).items():
            command += ["-e", key + "=" + str(value)]
        for source, target in mounts or []:
            command += ["--mount", "type=bind,src=" + str(source) + ",dst=" + target + ",readonly"]
        command += [args.image]
        if upstream:
            command += ["-g", "daemon off;", "-c", "/test-upstream.conf"]
        identifier = docker(*command).stdout.strip()
        containers.append(identifier)
        return identifier

    def inspect(identifier):
        return json.loads(docker("inspect", "--format",
            '{"state":{{json .State}},"ports":{{json .NetworkSettings.Ports}}}', identifier).stdout)

    def port(identifier, internal=80):
        return int(inspect(identifier)["ports"][str(internal) + "/tcp"][0]["HostPort"])

    def request(host_port, path="/", tls_context=None):
        url = ("https" if tls_context else "http") + "://127.0.0.1:" + str(host_port) + path
        handlers = [NoRedirect()]
        if tls_context:
            handlers.append(urllib.request.HTTPSHandler(context=tls_context))
        opener = urllib.request.build_opener(*handlers)
        req = urllib.request.Request(url, headers={"Authorization": "synthetic-smoke-token"})
        try:
            response = opener.open(req, timeout=5)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, dict(response.headers), response.read()

    def ready(identifier):
        for _ in range(50):
            state = inspect(identifier)["state"]
            if not state["Running"]:
                raise RuntimeError("isolated frontend exited before it became ready")
            try:
                if request(port(identifier), "/healthz")[0] == 200:
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.1)
        raise RuntimeError("isolated frontend did not become ready")

    try:
        docker("network", "create", "--label", "io.sxuoj.test=" + name, name)
        network_created = True
        upstream = root / "upstream.conf"
        upstream.write_text('events {}\nhttp { server { listen 8089; location /api/ { '
            'default_type application/json; add_header Cache-Control "public, max-age=900"; '
            'return 200 \'{"source":"test-upstream","token":"$http_authorization"}\'; } } }\n', encoding="utf-8")
        container("upstream", mounts=[(upstream, "/test-upstream.conf")], upstream=True)
        legacy = root / "legacy-static"
        legacy.mkdir()
        (legacy / "index.html").write_text("LEGACY-STATIC-MUST-NOT-BE-SERVED", encoding="utf-8")
        env = {"BACKEND_SERVER_HOST": "upstream", "BACKEND_SERVER_PORT": "8089",
               "SERVER_NAME": "localhost", "USE_HTTPS": "false"}
        app = container("legacy", env=env, mounts=[(legacy, "/usr/share/nginx/html")])
        ready(app)
        host_port = port(app)
        status, headers, body = request(host_port)
        passed("legacy_static_mount_does_not_shadow_new_image", status == 200 and b"<html" in body.lower()
               and b"LEGACY-STATIC" not in body)
        passed("spa_entry_is_not_cached", headers.get("Cache-Control") == "no-store")
        assets = re.findall(rb'(?:src|href)=["\'](/assets/[^"\']+)', body)
        passed("new_image_has_hashed_assets", bool(assets))
        passed("hashed_asset_is_served", request(host_port, assets[0].decode())[0] == 200)
        status, headers, manifest = request(host_port, '/version.json?test=uncached')
        version = json.loads(manifest)['version']
        passed("version_manifest_is_valid_and_not_cached", status == 200
               and bool(re.fullmatch('[a-f0-9]{32}', version)) and headers.get('Cache-Control') == 'no-store')
        app_script = next(asset.decode() for asset in assets if b'/js/app.' in asset and asset.endswith(b'.js'))
        passed("version_manifest_matches_running_application", version.encode() in request(host_port, app_script)[2])
        status, headers, body = request(host_port, '/contest/7/problems')
        passed("deep_links_use_uncached_html", status == 200 and b'<html' in body.lower()
               and headers.get('Cache-Control') == 'no-store')
        status, headers, body = request(host_port, "/api/ping")
        upstream_body = json.loads(body)
        passed("legacy_backend_environment_is_honored", status == 200 and upstream_body["source"] == "test-upstream")
        passed("authorization_header_survives_same_origin_proxy", upstream_body["token"] == "synthetic-smoke-token")
        passed("api_response_cannot_restore_shared_cache_header", headers.get("Cache-Control") == "no-store")
        status, headers, _ = request(host_port, "/scrollBoard?cid=7&removeStar=true")
        passed("scrollboard_redirect_keeps_query", status == 302 and "/scrollBoard/?cid=7&removeStar=true" in headers.get("Location", ""))
        passed("scrollboard_html_is_packaged", b"Scroll Board - HOJ" in request(host_port, "/scrollBoard/")[2])
        passed("scrollboard_script_is_packaged", request(host_port, "/scrollBoard/js/scrollboard.js")[0] == 200)
        passed("unversioned_scrollboard_script_is_not_cached", request(host_port, "/scrollBoard/js/scrollboard.js")[1].get('Cache-Control') == 'no-store')
        passed("scrollboard_styles_are_packaged", request(host_port, "/scrollBoard/css/scrollboard.css")[0] == 200)
        for suffix, changes in (("injection", {"BACKEND_UPSTREAM": "upstream:8089;return 200;"}),
                                ("bad-port", {"BACKEND_SERVER_PORT": "0"}),
                                ("no-cert", {"USE_HTTPS": "true"})):
            invalid = container(suffix, env={**env, **changes})
            for _ in range(50):
                state = inspect(invalid)["state"]
                if not state["Running"]:
                    break
                time.sleep(0.1)
            passed(suffix + "_fails_closed", not state["Running"] and state["ExitCode"] != 0)
        certificate, key = root / "fullchain.pem", root / "privkey.pem"
        result = subprocess.run([args.openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-keyout", str(key), "-out", str(certificate), "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"], capture_output=True, timeout=45)
        if result.returncode:
            raise RuntimeError("cannot generate a disposable local TLS certificate")
        secure = container("tls", env={**env, "USE_HTTPS": "true", "PUBLIC_HTTPS_PORT": "8082"},
            mounts=[(certificate, "/etc/nginx/certs/fullchain.pem"), (key, "/etc/nginx/certs/privkey.pem")], tls=True)
        ready(secure)
        status, headers, _ = request(port(secure), "/home?test=tls")
        passed("https_redirect_uses_public_port_and_keeps_path", status == 301
               and headers.get("Location") == "https://127.0.0.1:8082/home?test=tls")
        context = ssl.create_default_context(cafile=str(certificate))
        status, _, body = request(port(secure, 443), "/api/ping", context)
        passed("direct_https_serves_app_and_proxy", status == 200 and json.loads(body)["source"] == "test-upstream")
        helper = ["run", "--rm", "-i", "--network", "none", "--entrypoint", "java", args.backend_image,
                  "-cp", "/opt/hoj/tools:/opt/hoj/tools/hutool-all.jar", "NacosPasswordHash"]
        password = "synthetic-only-password-for-deployment-test"
        encoded = docker(*helper, stdin=password + "\n").stdout.strip()
        passed("offline_nacos_hash_is_bcrypt", bool(re.fullmatch(r"\$2[aby]\$10\$[./A-Za-z0-9]{53}", encoded)))
        passed("offline_nacos_hash_matches_input", docker(*helper, "--verify", stdin=password + "\n" + encoded + "\n").stdout.strip() == "true")
        rejected = docker(*helper, stdin="short\n", check=False)
        passed("offline_nacos_helper_rejects_weak_password", rejected.returncode != 0 and "short" not in rejected.stdout + rejected.stderr)
        print(json.dumps({"passed": len(assertions), "failed": 0, "assertions": assertions}))
    finally:
        for identifier in reversed(containers):
            labels = json.loads(docker("inspect", "--format", "{{json .Config.Labels}}", identifier).stdout)
            if labels.get("io.sxuoj.test") == name:
                docker("rm", "-f", identifier, check=False)
        if network_created:
            docker("network", "rm", name, check=False)
        if root.parent == Path(tempfile.gettempdir()).resolve() and root.name.startswith(name):
            shutil.rmtree(root)


if __name__ == "__main__":
    main()
