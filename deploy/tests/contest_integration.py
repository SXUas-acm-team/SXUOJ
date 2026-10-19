#!/usr/bin/env python3
"""Exercise confidential contests against an explicitly isolated loopback Docker stack.

Uses only Python's standard library and the Docker CLI. Secrets stay in memory and are
never included in the JSON report. Existing users and their rows are never modified.
The application and its two schema migrations must already be running.
"""
import argparse
import base64
import datetime as dt
import hashlib
import hmac
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class IntegrationError(RuntimeError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise IntegrationError("HTTP redirects are disabled for this isolated test")


def command(args, stdin=None):
    result = subprocess.run(args, input=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            encoding="utf-8", errors="replace", timeout=45, check=False)
    if result.returncode:
        mysql_error = re.search(r"ERROR\s+(\d+)\s+\(([A-Z0-9]+)\)", result.stderr)
        detail = " (MySQL %s/%s)" % mysql_error.groups() if mysql_error else ""
        raise IntegrationError("isolated Docker command failed" + detail)
    return result.stdout.strip()


def read_test_env(path):
    repo = Path(__file__).resolve().parents[2]
    permitted = (repo / ".tools" / "contest-test").resolve()
    resolved = path.resolve()
    if os.path.commonpath([str(permitted), str(resolved)]) != str(permitted):
        raise IntegrationError("environment file must be under .tools/contest-test")
    values = {}
    for line in resolved.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip("\"'")
    required = ("TEST_MYSQL_PASSWORD", "TEST_REDIS_PASSWORD", "TEST_JWT_SECRET")
    if not all(values.get(key) for key in required):
        raise IntegrationError("isolated environment is missing required TEST_ secrets")
    return values


def sql_value(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    encoded = str(value).encode("utf-8").hex()
    return "CONVERT(0x%s USING utf8mb4)" % encoded if encoded else "''"


class Database:
    TABLES = {"user_info", "user_role", "user_record", "role", "auth", "role_auth", "problem", "problem_count",
              "problem_language", "contest", "contest_problem", "announcement", "contest_announcement", "category",
              "discussion", "judge", "contest_record", "language", "group"}

    def __init__(self, container, password, database):
        self.container, self.password, self.database = container, password, database
        self.schemas = {}
        self.inserted = {}

    def query(self, sql):
        output = command(["docker", "exec", "-i", "-e", "MYSQL_PWD=" + self.password, self.container,
                          "mysql", "-h127.0.0.1", "-uroot", "--default-character-set=utf8mb4", "--batch", "--raw",
                          "--skip-column-names", self.database], sql + ";\n")
        return [line.split("\t") for line in output.splitlines()] if output else []

    def scalar(self, sql):
        rows = self.query(sql)
        if not rows or not rows[0]:
            raise IntegrationError("isolated metadata query returned no result")
        return rows[0][0]

    def columns(self, table):
        if table not in self.TABLES:
            raise IntegrationError("table is outside the synthetic fixture allowlist")
        if table not in self.schemas:
            self.schemas[table] = {row[0]: {"type": row[1], "nullable": row[2] == "YES", "key": row[3],
                                          "default": row[4], "extra": row[5] if len(row) > 5 else ""}
                                   for row in self.query("SHOW COLUMNS FROM `%s`" % table)}
        return self.schemas[table]

    def insert(self, table, supplied):
        columns = self.columns(table)
        values = {key: value for key, value in supplied.items() if key in columns}
        for name, metadata in columns.items():
            if name in values or metadata["nullable"] or metadata["default"] != "NULL" or "auto_increment" in metadata["extra"]:
                continue
            kind = metadata["type"].lower()
            if metadata["key"] == "PRI" and name == "id":
                values[name] = secrets.randbelow(1000000000) + 100000000
            elif any(word in kind for word in ("int", "decimal", "double", "float", "bit")):
                values[name] = 0
            elif "datetime" in kind or "timestamp" in kind:
                values[name] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            elif kind.startswith("enum("):
                values[name] = re.findall(r"'([^']*)'", kind)[0]
            else:
                values[name] = "{}" if kind == "json" else ""
        names = list(values)
        statement = "INSERT INTO `%s` (%s) VALUES (%s); SELECT LAST_INSERT_ID()" % (
            table, ",".join("`%s`" % name for name in names), ",".join(sql_value(values[name]) for name in names))
        try:
            rows = self.query(statement)
        except IntegrationError as error:
            raise IntegrationError("synthetic %s insert failed: %s" % (table, error)) from None
        row_id = values.get("id", int(rows[-1][0]) if rows else 0)
        self.inserted[table] = self.inserted.get(table, 0) + 1
        return row_id


class Suite:
    def __init__(self, args):
        self.args = args
        self.base = args.base_url.rstrip("/")
        self.prefix = args.container_prefix
        self.assertions, self.phase = [], "guard isolated environment"
        self.env = read_test_env(args.env_file)
        self.db = Database(self.prefix + "-mysql-1", self.env["TEST_MYSQL_PASSWORD"], args.database)
        self.http = urllib.request.build_opener(NoRedirect)
        self.run_id = "ctest_" + uuid.uuid4().hex[:10]
        self.users, self.tokens, self.contests = {}, {}, {}

    def check(self, name, condition):
        self.phase = name
        if not condition:
            raise IntegrationError("assertion failed")
        self.assertions.append(name)

    def guard(self):
        address = urllib.parse.urlsplit(self.base)
        if address.scheme != "http" or address.username or address.password or address.path not in ("", "/"):
            raise IntegrationError("only a plain loopback HTTP base URL is accepted")
        host = address.hostname or ""
        loopback = host == "localhost"
        try:
            loopback = loopback or ipaddress.ip_address(host).is_loopback
        except ValueError:
            pass
        if not loopback or not re.fullmatch(r"sxuoj-contest-test[0-9]+", self.prefix):
            raise IntegrationError("loopback host and sxuoj-contest-test<number> containers are mandatory")
        if not re.fullmatch(r"[A-Za-z0-9_]+", self.args.database):
            raise IntegrationError("invalid isolated database name")
        for suffix in ("mysql-1", "redis-1", "hoj-backend-1", "hoj-frontend-1"):
            container = self.prefix + "-" + suffix
            labels = json.loads(command(["docker", "inspect", "--format", "{{json .Config.Labels}}", container]))
            if labels.get("com.docker.compose.project") != self.prefix:
                raise IntegrationError("container does not belong to the isolated Compose project")
        bindings = json.loads(command(["docker", "inspect", "--format", "{{json .NetworkSettings.Ports}}",
                                       self.prefix + "-hoj-frontend-1"]))
        matches = [binding for items in bindings.values() if items for binding in items
                   if int(binding["HostPort"]) == (address.port or 80)]
        if not matches or any(not ipaddress.ip_address(item["HostIp"]).is_loopback for item in matches):
            raise IntegrationError("isolated frontend proxy must be published only on loopback")
        self.check("isolated_loopback_stack_verified", True)

    def redis(self, *arguments):
        return command(["docker", "exec", "-e", "REDISCLI_AUTH=" + self.env["TEST_REDIS_PASSWORD"],
                        self.prefix + "-redis-1", "redis-cli", "--no-auth-warning", *map(str, arguments)])

    def token(self, uid):
        epoch, session_id = base64.b64encode(secrets.token_bytes(64)).decode(), uuid.uuid4().hex
        def b64(value):
            return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).rstrip(b"=").decode()
        now = int(time.time())
        unsigned = b64({"alg": "HS512", "typ": "JWT"}) + "." + b64({"sub": uid, "jti": session_id,
                     "sessionEpoch": epoch, "iat": now, "exp": now + 3600})
        signature = hmac.new(base64.b64decode(self.env["TEST_JWT_SECRET"]), unsigned.encode(), hashlib.sha512).digest()
        token = unsigned + "." + base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
        prefix = "shiro:token-key:uid:"
        self.redis("SET", prefix + uid, json.dumps(epoch), "EX", "3600")
        self.redis("SET", prefix + "session:" + uid + ":" + session_id, json.dumps(hashlib.sha256(token.encode()).hexdigest()), "EX", "3600")
        self.redis("SET", "shiro:token-refresh:uid:" + uid + ":" + session_id, json.dumps("1"), "EX", "3600")
        return token

    def request(self, path, actor=None, payload=None, method=None, binary=False, headers=None):
        url = urllib.parse.urljoin(self.base + "/", path)
        target, base = urllib.parse.urlsplit(url), urllib.parse.urlsplit(self.base)
        if target.scheme != base.scheme or target.netloc != base.netloc:
            raise IntegrationError("test request attempted to leave its isolated origin")
        request_headers = {"Accept": "application/json"}
        if actor:
            request_headers["Authorization"] = self.tokens[actor]
        if headers:
            request_headers.update(headers)
        data = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        if data is not None:
            request_headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=request_headers, method=method or ("POST" if data else "GET"))
        try:
            response = self.http.open(req, timeout=25)
        except urllib.error.HTTPError as error:
            response = error
        except (urllib.error.URLError, TimeoutError):
            raise IntegrationError("isolated HTTP request could not complete") from None
        with response:
            raw, status = response.read(), response.code
        if binary:
            return status, raw
        try:
            body = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeError):
            raise IntegrationError("API did not return its JSON envelope") from None
        return status, body

    def success(self, name, path, actor=None, payload=None, method=None):
        status, body = self.request(path, actor, payload, method)
        self.check(name, status == 200 and body.get("status") == 200)
        return body.get("data")

    def denied(self, name, path, actor=None, payload=None, method=None):
        status, body = self.request(path, actor, payload, method)
        self.check(name, status < 500 and body.get("status") in (400, 401, 403, 404) and body.get("data") is None)

    def root_update(self, cid, **changes):
        data = self.success("admin_can_load_synthetic_contest", "/api/admin/contest?cid=" + str(cid), "root")
        data.update(changes)
        self.success("admin_can_save_synthetic_contest", "/api/admin/contest", "root", data, "PUT")

    def seed(self):
        self.phase = "create synthetic identities and roles"
        try:
            auth_id = self.db.insert("auth", {"id": secrets.randbelow(1000000000) + 100000000,
                                   "name": self.run_id + " submit", "permission": "submit", "status": 0})
        except IntegrationError:
            auth_id = int(self.db.scalar("SELECT id FROM auth WHERE permission='submit' ORDER BY id LIMIT 1"))
        for actor in ("root", "student", "unregistered"):
            role_id = secrets.randbelow(1000000000) + 100000000
            role_name = "root" if actor == "root" else self.run_id + "_" + actor
            try:
                self.db.insert("role", {"id": role_id, "role": role_name, "description": self.run_id, "status": 0})
            except IntegrationError:
                if actor != "root":
                    raise
                role_id = int(self.db.scalar("SELECT id FROM role WHERE role='root' ORDER BY id LIMIT 1"))
            else:
                self.db.insert("role_auth", {"role_id": role_id, "auth_id": auth_id})
            uid = uuid.uuid4().hex
            name = self.run_id + "_" + actor
            self.db.insert("user_info", {"uuid": uid, "username": name, "password": "synthetic JWT-only fixture",
                                         "nickname": name, "realname": name, "status": 0})
            self.db.insert("user_record", {"uid": uid})
            self.db.insert("user_role", {"uid": uid, "role_id": role_id})
            self.users[actor] = uid
            self.tokens[actor] = self.token(uid)
        self.problem_name = self.run_id.upper() + "_P"
        self.asset_name = self.run_id + ".png"
        self.raw_asset = "/api/public/img/" + self.asset_name
        self.marker = self.run_id + "_STATEMENT"
        self.pid = self.db.insert("problem", {"problem_id": self.problem_name, "title": self.marker, "author": self.run_id + "_root",
            "type": 0, "judge_mode": "default", "judge_case_mode": "default", "time_limit": 1000,
            "memory_limit": 128, "stack_limit": 128, "description": self.marker + " ![private](" + self.raw_asset + ")",
            "input": "synthetic input", "output": "synthetic output", "examples": "[]", "auth": 1, "is_remote": False,
            "is_group": False, "code_share": True, "open_case_result": True, "is_remove_end_blank": True,
            "is_upload_case": True, "case_version": "synthetic", "is_file_io": False})
        language = self.db.query("SELECT id,name FROM language WHERE name IN ('C++','C','Python3') ORDER BY id LIMIT 1")
        if not language:
            raise IntegrationError("isolated backup has no supported language metadata")
        self.language = language[0][1]
        self.db.insert("problem_language", {"pid": self.pid, "lid": int(language[0][0])})
        now = dt.datetime.now(dt.timezone.utc)
        for name in ("main", "old", "second"):
            start, end = now - dt.timedelta(hours=2), now - dt.timedelta(hours=1)
            cid = self.db.insert("contest", {"uid": self.users["root"], "author": self.run_id + "_root",
                "title": self.run_id + "_" + name, "type": 0, "description": "private contest description " + self.raw_asset,
                "auth": 0, "pwd": "synthetic-password", "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end.strftime("%Y-%m-%d %H:%M:%S"), "duration": 3600, "status": 1, "visible": True,
                "seal_rank": False, "auto_real_rank": True, "open_print": False, "open_account_limit": False,
                "rank_show_name": "username", "star_account": '{"star_account":[]}', "open_rank": True,
                "oi_rank_score_type": "Highest", "is_group": False, "award_type": 0, "allow_end_submit": True})
            self.contests[name] = cid
            self.db.insert("contest_problem", {"cid": cid, "pid": self.pid, "display_id": "A", "display_title": self.marker,
                                               "color": "#336699"})
        announcement = self.db.insert("announcement", {"title": self.run_id + "_announcement", "uid": self.users["root"],
                                        "content": self.marker + " announcement " + self.raw_asset, "status": 0})
        self.db.insert("contest_announcement", {"cid": self.contests["main"], "aid": announcement})
        category = self.db.insert("category", {"name": self.run_id, "is_group": False})
        self.did = self.db.insert("discussion", {"uid": self.users["student"], "author": self.run_id + "_student",
                         "title": self.run_id + "_editorial", "description": "synthetic answer", "content": self.marker,
                         "pid": self.problem_name, "category_id": category, "status": 0, "role": "user", "top_priority": False})
        self.legacy_submission = self.db.insert("judge", {"uid": self.users["student"], "username": self.run_id + "_student",
            "pid": self.pid, "cid": 0, "cpid": 0, "display_pid": self.problem_name, "status": 0, "share": True,
            "code": "synthetic legacy answer", "language": self.language, "length": 23, "version": 0})
        png = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aVxQAAAAASUVORK5CYII=")
        command(["docker", "exec", self.prefix + "-hoj-backend-1", "mkdir", "-p", "/hoj/file/md"])
        proc = subprocess.run(["docker", "exec", "-i", self.prefix + "-hoj-backend-1", "sh", "-c",
                               "cat > /hoj/file/md/" + self.asset_name], input=png, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              timeout=20, check=False)
        if proc.returncode:
            raise IntegrationError("cannot install synthetic attachment in isolated backend")
        self.check("synthetic_only_fixture_created", True)

    def run(self):
        self.guard()
        self.seed()
        cid, old, second = self.contests["main"], self.contests["old"], self.contests["second"]
        detail = lambda contest=cid: "/api/get-contest-problem-details?cid=%s&displayId=A" % contest
        rank = lambda contest=cid, **extra: dict(cid=contest, limit=10, currentPage=1, forceRefresh=True,
                                               removeStar=False, containsEnd=True, concernedList=[], **extra)
        now = lambda: int(time.time() * 1000)
        old_info = self.success("legacy_outside_info_can_be_primed", "/api/get-contest-outsize-info?cid=" + str(old), "student")
        self.check("legacy_outside_info_initially_contains_fixture_problem", any(str(row.get("pid")) == str(self.pid)
                    for row in old_info.get("problemList", [])))
        self.success("public_problem_baseline_available", "/api/get-problem-detail?problemId=" + self.problem_name)
        self.success("public_editorial_baseline_available", "/api/get-discussion-detail?did=" + str(self.did), "student")
        self.success("shared_submission_baseline_available", "/api/get-submission-detail?submitId=" + str(self.legacy_submission), "student")
        self.success("outside_rank_cache_baseline_can_be_primed", "/api/get-contest-outside-scoreboard", "student", rank())
        status, asset = self.request(self.raw_asset, binary=True)
        self.check("synthetic_attachment_baseline_available", status == 200 and asset.startswith(b"\x89PNG"))
        self.root_update(cid, confidential=True, releaseAt=None, pwd="synthetic-password", startTime=now() + 300000,
                         endTime=now() + 600000, status=-1, duration=300)
        self.denied("before_start_anonymous_statement_denied", detail())
        self.denied("before_start_admin_password_does_not_bypass_clock_for_student", detail(), "student")
        info = self.success("anonymous_contest_shell_available", "/api/get-contest-info?cid=" + str(cid))
        self.check("contest_shell_does_not_expose_description", info.get("description") is None)
        self.root_update(cid, confidential=True, releaseAt=None, startTime=now() - 120000, endTime=now() + 600000,
                         status=0, duration=720)
        self.denied("during_contest_anonymous_problem_list_denied", "/api/get-contest-problem?cid=" + str(cid))
        access = self.success("anonymous_access_state_available", "/api/get-contest-access?cid=" + str(cid))
        self.check("anonymous_access_state_denies_view_and_submit", not access.get("canViewProblems") and not access.get("canSubmit"))
        self.denied("during_contest_unregistered_statement_denied", detail(), "student")
        self.denied("old_outside_rank_cache_does_not_bypass_new_policy", "/api/get-contest-outside-scoreboard", "unregistered", rank())
        self.denied("ordinary_user_cannot_modify_confidential_policy", "/api/admin/contest", "student", {"id": cid}, "PUT")
        self.denied("wrong_password_rejected", "/api/register-contest", "student", {"cid": cid, "password": "wrong"})
        self.success("correct_password_accepted", "/api/register-contest", "student", {"cid": cid, "password": "synthetic-password"})
        access = self.success("registered_access_state_available", "/api/get-contest-access?cid=" + str(cid), "student")
        self.check("registered_access_state_allows_view_and_submit", access.get("canViewProblems") is True and access.get("canSubmit") is True)
        statement = self.success("registered_participant_reads_statement", detail(), "student")
        self.check("statement_payload_is_synthetic_fixture", self.marker in statement["problem"]["description"])
        capability = re.search(r"(/api/public/(?:img|file)/[^\s)\"']+contestResourceToken=[A-Za-z0-9_-]+)",
                               statement["problem"]["description"])
        self.check("statement_contains_scoped_resource_capability", capability is not None)
        capability_url = capability.group(1)
        status, asset = self.request(capability_url, binary=True)
        self.check("validated_resource_capability_streams_attachment", status == 200 and asset.startswith(b"\x89PNG"))
        for path, label in ((self.raw_asset, "image"), (self.raw_asset.replace("/img/", "/file/"), "file_alias"),
                            (self.raw_asset.upper().replace("/API/PUBLIC/IMG/", "/api/public/img/"), "filename_case_alias")):
            status, _ = self.request(path, binary=True)
            self.check("bare_" + label + "_denied", status == 403)
        for method in ("HEAD", "GET"):
            status, _ = self.request(self.raw_asset, method=method, binary=True,
                                     headers={"Range": "bytes=0-3", "If-None-Match": '"cached-before-confidential"'})
            self.check("conditional_range_" + method.lower() + "_does_not_bypass_attachment_lock", status == 403)
        self.success("registered_participant_reads_rank", "/api/get-contest-rank", "student", rank())
        self.success("registered_participant_reads_announcements", "/api/get-contest-announcement?cid=" + str(cid), "student")
        self.denied("public_problem_route_cannot_bypass_embargo", "/api/get-problem-detail?problemId=" + self.problem_name)
        self.denied("public_editorial_cannot_bypass_embargo", "/api/get-discussion-detail?did=" + str(self.did), "student")
        self.denied("legacy_shared_submission_cannot_bypass_embargo", "/api/get-submission-detail?submitId=" + str(self.legacy_submission), "student")
        self.denied("old_public_contest_cannot_bypass_shared_problem", detail(old), "student")
        old_info = self.success("old_outside_info_request_still_returns_shell", "/api/get-contest-outsize-info?cid=" + str(old), "student")
        self.check("old_outside_info_excludes_embargoed_problem", not any(str(row.get("pid")) == str(self.pid) for row in old_info.get("problemList", [])))
        self.root_update(cid, confidential=True, releaseAt=None, pwd="changed-synthetic-password")
        self.denied("password_change_invalidates_old_registration", detail(), "student")
        status, _ = self.request(capability_url, binary=True)
        self.check("password_change_invalidates_resource_capability", status == 403)
        self.success("changed_password_can_be_verified", "/api/register-contest", "student",
                     {"cid": cid, "password": "changed-synthetic-password"})
        statement = self.success("new_registration_restores_statement", detail(), "student")
        capability_url = re.search(r"(/api/public/(?:img|file)/[^\s)\"']+contestResourceToken=[A-Za-z0-9_-]+)", statement["problem"]["description"]).group(1)
        natural_end = now() + 6000
        self.root_update(cid, confidential=True, releaseAt=None, startTime=now() - 600000, endTime=natural_end,
                         status=0, duration=606)
        self.success("rescheduled_sitting_requires_fresh_password", "/api/register-contest", "student",
                     {"cid": cid, "password": "changed-synthetic-password"})
        statement = self.success("participant_reads_before_natural_end", detail(), "student")
        capability_url = re.search(r"(/api/public/(?:img|file)/[^\s)\"']+contestResourceToken=[A-Za-z0-9_-]+)", statement["problem"]["description"]).group(1)
        remaining = natural_end / 1000 - time.time() + 0.3
        if remaining > 0:
            time.sleep(min(remaining, 8))
        for actor in (None, "student", "unregistered"):
            self.denied("after_end_statement_sealed_for_" + (actor or "anonymous"), detail(), actor)
        self.denied("after_end_rank_sealed", "/api/get-contest-rank", "student", rank())
        access = self.success("after_end_access_state_available", "/api/get-contest-access?cid=" + str(cid), "student")
        self.check("after_end_access_state_denies_view_and_submit", not access.get("canViewProblems") and not access.get("canSubmit"))
        self.denied("after_end_announcements_sealed", "/api/get-contest-announcement?cid=" + str(cid), "student")
        self.denied("after_end_outside_rank_sealed", "/api/get-contest-outsize-info?cid=" + str(cid), "student")
        self.denied("after_end_own_ac_code_sealed", "/api/get-last-ac-code?cid=%s&pid=%s" % (cid, self.pid), "student")
        status, _ = self.request(capability_url, binary=True)
        self.check("end_time_immediately_revokes_resource_capability", status == 403)
        self.success("administrator_can_still_read_sealed_statement", detail(), "root")
        self.root_update(cid, confidential=True, releaseAt=now() - 1000)
        self.success("manual_publication_allows_anonymous_statement", detail())
        self.success("manual_publication_allows_anonymous_problem_list", "/api/get-contest-problem?cid=" + str(cid))
        self.success("manual_publication_allows_anonymous_rank", "/api/get-contest-rank", payload=rank())
        self.success("manual_publication_allows_anonymous_announcements", "/api/get-contest-announcement?cid=" + str(cid))
        self.success("manual_publication_allows_anonymous_outside_info", "/api/get-contest-outsize-info?cid=" + str(cid))
        self.success("manual_publication_allows_anonymous_outside_rank", "/api/get-contest-outside-scoreboard", payload=rank())
        access = self.success("published_anonymous_access_state_available", "/api/get-contest-access?cid=" + str(cid))
        self.check("published_anonymous_access_allows_view_but_still_requires_login_to_submit",
                   access.get("canViewProblems") is True and not access.get("canSubmit"))
        access = self.success("published_unregistered_access_state_available", "/api/get-contest-access?cid=" + str(cid), "unregistered")
        self.check("published_unregistered_access_allows_supplementary_submission", access.get("canSubmit") is True)
        status, _ = self.request(self.raw_asset, binary=True)
        self.check("manual_publication_restores_bare_attachment", status == 200)
        self.check("post_publication_user_has_never_registered", self.db.scalar("SELECT COUNT(*) FROM contest_register WHERE cid=%s AND uid=%s" %
                    (cid, sql_value(self.users["unregistered"]))) == "0")
        submission = self.success("unregistered_logged_in_user_can_submit_one_character_after_publication",
                    "/api/submit-problem-judge", "unregistered", {"pid": "A", "language": self.language, "code": "x", "cid": cid,
                    "tid": 0, "gid": None, "isRemote": False})
        submit_id = submission["submitId"]
        self.check("post_publication_submission_stored_with_exact_one_character_code", self.db.scalar(
            "SELECT COUNT(*) FROM judge WHERE submit_id=%s AND uid=%s AND cid=%s AND code='x' AND length=1" %
            (submit_id, sql_value(self.users["unregistered"]), cid)) == "1")
        self.check("post_publication_submission_has_contest_record", self.db.scalar(
            "SELECT COUNT(*) FROM contest_record WHERE submit_id=%s AND uid=%s AND cid=%s" %
            (submit_id, sql_value(self.users["unregistered"]), cid)) == "1")
        self.check("submission_was_accepted_for_judging_not_claimed_accepted_solution", submission.get("status") in (4, 5, 6, 7, 10))
        self.root_update(cid, confidential=True, releaseAt=None)
        self.denied("reseal_immediately_hides_statement", detail(), "student")
        status, _ = self.request(self.raw_asset, binary=True)
        self.check("reseal_immediately_hides_bare_attachment", status == 403)
        release = now() + 3000
        self.root_update(cid, confidential=True, releaseAt=release)
        self.denied("scheduled_publication_is_not_early", detail())
        remaining = release / 1000 - time.time() + 0.3
        if remaining > 0:
            time.sleep(min(remaining, 5))
        self.success("scheduled_publication_releases_without_scheduler", detail())
        self.root_update(second, confidential=True, releaseAt=None, startTime=now() + 300000,
                         endTime=now() + 600000, status=-1, duration=300)
        self.denied("published_old_sitting_cannot_bypass_second_unreleased_sitting", detail(), "student")
        status, _ = self.request(self.raw_asset, binary=True)
        self.check("published_resource_remains_locked_by_second_sitting", status == 403)
        self.denied("external_rank_merge_cannot_bypass_unreleased_sitting", "/api/get-contest-rank", "student",
                    rank(externalCidList=[second]))
        self.root_update(second, confidential=True, releaseAt=None, startTime=now() - 120000,
                         endTime=now() + 300000, status=0, duration=420)
        self.success("second_sitting_password_accepted", "/api/register-contest", "student",
                     {"cid": second, "password": "synthetic-password"})
        self.success("authorized_live_second_sitting_can_read_shared_problem", detail(second), "student")
        self.success("administrator_can_remove_synthetic_contest_problem", "/api/admin/contest/problem?pid=%s&cid=%s" % (self.pid, second),
                     "root", method="DELETE")
        self.denied("removing_problem_does_not_remove_its_historical_embargo", detail(), "student")
        self.root_update(second, confidential=True, releaseAt=now() - 1000, startTime=now() - 600000,
                         endTime=now() - 10000, status=1, duration=590)
        self.success("all_sittings_released_restores_anonymous_statement", detail())
        return {"passed": len(self.assertions), "failed": 0, "assertions": self.assertions,
                "synthetic_rows_inserted": self.db.inserted, "judging": "submission and contest_record persisted; no AC verdict asserted"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:16688")
    parser.add_argument("--container-prefix", default="sxuoj-contest-test8")
    parser.add_argument("--database", default="hoj")
    parser.add_argument("--env-file", type=Path, default=Path(__file__).resolve().parents[2] / ".tools/contest-test/test8.env")
    args = parser.parse_args()
    suite = None
    try:
        suite = Suite(args)
        report = suite.run()
    except Exception as error:
        report = {"passed": len(suite.assertions) if suite else 0, "failed": 1,
                  "failed_at": suite.phase if suite else "isolated environment configuration", "error_type": type(error).__name__,
                  "assertions": suite.assertions if suite else []}
        if isinstance(error, IntegrationError):
            report["reason"] = str(error)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
