"""Offline credential preparation tests. Every Docker call is an explicit fake."""

import base64
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "prepare_upgrade.py"
SPEC = importlib.util.spec_from_file_location("sxuoj_prepare_upgrade", SCRIPT)
upgrade = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(upgrade)
JWT = base64.b64encode(bytes(range(64))).decode("ascii")
JUDGE = "synthetic-judge-token-0123456789-abcdef"
NACOS = "synthetic-existing-nacos-password"
OLD_HASH = "$2a$10$" + "A" * 53
NEW_HASH = "$2a$10$" + "B" * 53


class FakeDocker:
    def __init__(self):
        self.calls = []
        self.rows = "operator".encode().hex() + "\t" + OLD_HASH.encode().hex() + "\n"
        self.hash_output = NEW_HASH + "\n"
        self.failure = None

    def __call__(self, command, **options):
        self.calls.append((command, options))
        if self.failure:
            raise self.failure
        if command[:4] == ["docker", "exec", "-i", "synthetic-mysql"]:
            return subprocess.CompletedProcess(command, 0, self.rows, "hidden warning")
        if command[:12] == ["docker", "run", "--rm", "-i", "--network", "none", "--pull", "never", "--entrypoint", "java", "synthetic-backend:test", "-cp"]:
            return subprocess.CompletedProcess(command, 0, self.hash_output, "")
        raise AssertionError("Unexpected Docker operation")


class PrepareUpgradeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env = Path(self.temp.name) / ".env"
        self.fake = FakeDocker()

    def credentials(self, jwt=JWT, judge=JUDGE, nacos=NACOS):
        return "JWT_TOKEN_SECRET=" + jwt + "\nJUDGE_TOKEN=" + judge + "\nNACOS_PASSWORD=" + nacos + "\nNACOS_USERNAME=operator\n"

    def invoke(self, *extra):
        output = io.StringIO()
        errors = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors), mock.patch.object(upgrade.subprocess, "run", self.fake):
            result = upgrade.main(["--env-file", str(self.env), "--backend-image", "synthetic-backend:test",
                                   "--mysql-container", "synthetic-mysql", *extra])
        return result, output.getvalue(), errors.getvalue()

    def package(self):
        return next((self.env.parent / ".sxuoj-upgrade").iterdir())

    def assert_no_secrets(self, output, *additional):
        for secret in (JWT, JUDGE, NACOS, OLD_HASH, NEW_HASH, *additional):
            self.assertNotIn(secret, output)

    def test_default_is_boolean_only_and_performs_no_docker_or_writes(self):
        original = self.credentials().encode()
        self.env.write_bytes(original)
        code, output, error = self.invoke()
        self.assertEqual(0, code)
        self.assertTrue(all(type(value) is bool for value in json.loads(output).values()))
        self.assertTrue(all(json.loads(output).values()))
        self.assertEqual(original, self.env.read_bytes())
        self.assertEqual([], self.fake.calls)
        self.assertFalse((self.env.parent / ".sxuoj-upgrade").exists())
        self.assert_no_secrets(output + error)

    def test_all_strong_is_noop_even_in_write_mode(self):
        original = self.credentials().encode()
        self.env.write_bytes(original)
        code, output, _ = self.invoke("--write")
        self.assertEqual(0, code)
        self.assertFalse(json.loads(output)["env_updated"])
        self.assertEqual(original, self.env.read_bytes())
        self.assertEqual([], self.fake.calls)
        self.assertFalse((self.env.parent / ".sxuoj-upgrade").exists())

    def test_rotation_preserves_other_lines_comments_bom_and_newlines(self):
        original = ("\ufeff# leave this comment\r\nJWT_TOKEN_SECRET=default  # JWT comment\r\n"
                    "JUDGE_TOKEN=short # judge comment\r\nNACOS_PASSWORD=" + NACOS +
                    "\r\nexport OTHER = 'literal $value # kept'\r\nPORT=6688\r\n").encode("utf-8")
        self.env.write_bytes(original)
        code, output, error = self.invoke("--write")
        self.assertEqual(0, code)
        updated = self.env.read_bytes()
        self.assertTrue(updated.startswith(b"\xef\xbb\xbf# leave this comment\r\n"))
        self.assertIn(b"  # JWT comment\r\n", updated)
        self.assertIn(b" # judge comment\r\n", updated)
        self.assertIn(b"export OTHER = 'literal $value # kept'\r\nPORT=6688\r\n", updated)
        values = upgrade.EnvDocument(updated).values
        self.assertTrue(upgrade.strong_jwt(values["JWT_TOKEN_SECRET"]))
        self.assertTrue(upgrade.strong_judge(values["JUDGE_TOKEN"]))
        self.assertEqual(NACOS, values["NACOS_PASSWORD"])
        self.assertEqual(original, (self.package() / "previous.env").read_bytes())
        self.assertEqual([], self.fake.calls)
        self.assert_no_secrets(output + error, values["JWT_TOKEN_SECRET"], values["JUDGE_TOKEN"])

    def test_existing_strong_canonical_is_reused_for_weak_priority_alias(self):
        self.env.write_text(self.credentials() + "HOJ_JWT_SECRET=default\nHOJ_JUDGE_TOKEN=weak\n", encoding="utf-8")
        self.assertEqual(0, self.invoke("--write")[0])
        values = upgrade.EnvDocument(self.env.read_bytes()).values
        self.assertEqual(JWT, values["JWT_TOKEN_SECRET"])
        self.assertEqual(JWT, values["HOJ_JWT_SECRET"])
        self.assertEqual(JUDGE, values["JUDGE_TOKEN"])
        self.assertEqual(JUDGE, values["HOJ_JUDGE_TOKEN"])

    def test_strong_priority_alias_is_reused_for_weak_canonical(self):
        self.env.write_text(self.credentials(jwt="default", judge="weak") + "HOJ_JWT_SECRET=" + JWT + "\nHOJ_JUDGE_TOKEN=" + JUDGE + "\n", encoding="utf-8")
        self.assertEqual(0, self.invoke("--write")[0])
        values = upgrade.EnvDocument(self.env.read_bytes()).values
        self.assertEqual(JWT, values["JWT_TOKEN_SECRET"])
        self.assertEqual(JUDGE, values["JUDGE_TOKEN"])

    def test_two_different_strong_aliases_refuse_write(self):
        original = self.credentials() + "HOJ_JUDGE_TOKEN=another-strong-judge-token-0123456789\n"
        self.env.write_text(original, encoding="utf-8")
        code, output, error = self.invoke("--write")
        self.assertEqual(1, code)
        self.assertEqual(original, self.env.read_text())
        self.assertEqual([], self.fake.calls)
        self.assert_no_secrets(output + error, "another-strong-judge-token-0123456789")

    def test_unpadded_standard_base64_strong_jwt_remains_unchanged(self):
        original = self.credentials(jwt=JWT.rstrip("="))
        self.env.write_text(original, encoding="utf-8")
        self.assertEqual(0, self.invoke("--write")[0])
        self.assertEqual(original, self.env.read_text())

    def test_nacos_prepares_precise_guarded_sql_without_executing_it(self):
        original = self.credentials(nacos="nacos").encode()
        self.env.write_bytes(original)
        code, output, error = self.invoke("--write", "--nacos-database", "nacos_test")
        self.assertEqual(0, code)
        values = upgrade.EnvDocument(self.env.read_bytes()).values
        self.assertEqual(32, len(values["NACOS_PASSWORD"]))
        self.assertEqual(2, len(self.fake.calls))
        read, hash_call = self.fake.calls
        self.assertIn("MYSQL_PWD=\"$MYSQL_ROOT_PASSWORD\"", read[0][-1])
        self.assertTrue(read[1]["input"].startswith("SELECT HEX(username),HEX(password) FROM `nacos_test`.`users` WHERE BINARY username=UNHEX('"))
        self.assertNotIn("UPDATE", read[1]["input"])
        self.assertEqual(values["NACOS_PASSWORD"] + "\n", hash_call[1]["input"])
        self.assertEqual(["--rm", "-i", "--network", "none", "--pull", "never", "--entrypoint", "java"], hash_call[0][2:10])
        self.assertEqual("NacosPasswordHash", hash_call[0][-1])
        sql = (self.package() / "nacos-password.sql").read_text()
        rollback = (self.package() / "rollback-nacos-password.sql").read_text()
        self.assertIn("UPDATE `nacos_test`.`users`", sql)
        self.assertIn("BINARY username=UNHEX('" + "operator".encode().hex() + "')", sql)
        self.assertIn("BINARY password=UNHEX('" + OLD_HASH.encode().hex() + "')", sql)
        self.assertIn("BINARY password=UNHEX('" + NEW_HASH.encode().hex() + "')", rollback)
        self.assertNotIn("INSERT", sql)
        self.assertNotIn("GRANT", sql)
        self.assertNotIn(values["NACOS_PASSWORD"], sql + rollback)
        self.assertIn("every JudgeServer", output)
        self.assert_no_secrets(output + error, values["NACOS_PASSWORD"])

    def test_missing_or_duplicate_nacos_account_refuses_before_env_write(self):
        for rows in ("", self.fake.rows * 2):
            with self.subTest(rows_count=len(rows.splitlines())):
                original = self.credentials(nacos="nacos").encode()
                self.env.write_bytes(original)
                self.fake.rows = rows
                code, output, error = self.invoke("--write")
                self.assertEqual(1, code)
                self.assertEqual(original, self.env.read_bytes())
                self.assert_no_secrets(output + error)

    def test_subprocess_failure_never_prints_command_stdin_or_external_output(self):
        self.env.write_text(self.credentials(nacos="nacos"), encoding="utf-8")
        original = self.env.read_bytes()
        self.fake.failure = subprocess.CalledProcessError(1, ["docker", "private-command"], output="private-out", stderr="private-secret")
        code, output, error = self.invoke("--write")
        self.assertEqual(1, code)
        self.assertEqual(original, self.env.read_bytes())
        for private in ("private-command", "private-out", "private-secret"):
            self.assertNotIn(private, output + error)

    def test_invalid_helper_output_refuses_before_env_write(self):
        self.env.write_text(self.credentials(nacos="nacos"), encoding="utf-8")
        original = self.env.read_bytes()
        self.fake.hash_output = "private-helper-error\n" + NEW_HASH
        code, output, error = self.invoke("--write")
        self.assertEqual(1, code)
        self.assertEqual(original, self.env.read_bytes())
        self.assertNotIn("private-helper-error", output + error)

    def test_sql_account_name_is_encoded_as_data_even_when_it_contains_quotes(self):
        username = "operator' OR 1=1 --"
        self.env.write_text(self.credentials(nacos="nacos").replace("NACOS_USERNAME=operator", 'NACOS_USERNAME="' + username + '"'), encoding="utf-8")
        self.fake.rows = username.encode().hex() + "\t" + OLD_HASH.encode().hex() + "\n"
        self.assertEqual(0, self.invoke("--write")[0])
        query = self.fake.calls[0][1]["input"]
        sql = (self.package() / "nacos-password.sql").read_text()
        self.assertIn(username.encode().hex(), query)
        self.assertNotIn(username, query + sql)

    def test_invalid_sql_identifier_or_container_is_rejected_before_docker(self):
        self.env.write_text(self.credentials(nacos="nacos"), encoding="utf-8")
        original = self.env.read_bytes()
        for arguments in (("--nacos-database", "nacos; DROP TABLE users"), ("--mysql-container", "--privileged")):
            with self.subTest(argument=arguments[0]):
                # Use = form for values starting with '-' so argparse treats them as a value.
                code, output, error = self.invoke("--write", arguments[0] + "=" + arguments[1])
                self.assertEqual(1, code)
                self.assertEqual(original, self.env.read_bytes())
                self.assertNotIn(arguments[1], output + error)
        self.assertEqual([], self.fake.calls)

    def test_backup_failure_cannot_modify_the_environment(self):
        self.env.write_text(self.credentials(jwt="default"), encoding="utf-8")
        original = self.env.read_bytes()
        with mock.patch.object(upgrade, "private_write", side_effect=PermissionError("private-backup-path")):
            code, output, error = self.invoke("--write")
        self.assertEqual(1, code)
        self.assertEqual(original, self.env.read_bytes())
        self.assertNotIn("private-backup-path", output + error)

    def test_production_nacos_cannot_be_disabled_by_preparation(self):
        self.env.write_text(self.credentials(jwt="default") + "NACOS_ENABLED=false\n", encoding="utf-8")
        original = self.env.read_bytes()
        self.assertEqual(1, self.invoke("--write")[0])
        self.assertEqual(original, self.env.read_bytes())
        self.assertEqual([], self.fake.calls)

    def test_interpolation_and_duplicate_assignments_are_rejected_without_disclosure(self):
        for extra in ('OTHER=${PRIVATE_FALLBACK:-sensitive-value}\n', 'OTHER="$PRIVATE_VALUE"\n', 'JUDGE_TOKEN=another-sensitive-value\n'):
            with self.subTest(extra_kind=extra.split("=", 1)[0]):
                self.env.write_text(self.credentials() + extra, encoding="utf-8")
                original = self.env.read_bytes()
                code, output, error = self.invoke("--write")
                self.assertEqual(1, code)
                self.assertEqual(original, self.env.read_bytes())
                self.assertNotIn("sensitive-value", output + error)
                self.assertNotIn("PRIVATE_VALUE", output + error)
        self.assertEqual([], self.fake.calls)

    def test_literal_dollar_escaping_is_supported_without_environment_lookup(self):
        document = upgrade.EnvDocument(b"ONE='literal $VAR'\nTWO=literal$$VAR\nTHREE=\"literal$$VAR\"\n")
        self.assertEqual({"ONE": "literal $VAR", "TWO": "literal$VAR", "THREE": "literal$VAR"}, document.values)

    def test_replace_failure_keeps_original_and_retains_private_backup(self):
        self.env.write_text(self.credentials(jwt="default"), encoding="utf-8")
        original = self.env.read_bytes()
        with mock.patch.object(upgrade.os, "replace", side_effect=PermissionError("private-path")):
            code, output, error = self.invoke("--write")
        self.assertEqual(1, code)
        self.assertEqual(original, self.env.read_bytes())
        self.assertEqual(original, (self.package() / "previous.env").read_bytes())
        self.assertNotIn("private-path", output + error)
        self.assertFalse(list(self.env.parent.glob("..env.upgrade-*")))

    def test_concurrent_env_change_is_not_overwritten(self):
        self.env.write_text(self.credentials(jwt="default"), encoding="utf-8")
        original_write = upgrade.private_write
        def concurrent(path, content):
            original_write(path, content)
            if ".upgrade-" in path.name:
                self.env.write_text("changed-by-another-operator\n", encoding="utf-8")
        with mock.patch.object(upgrade, "private_write", side_effect=concurrent):
            code, output, error = self.invoke("--write")
        self.assertEqual(1, code)
        self.assertEqual("changed-by-another-operator\n", self.env.read_text())
        self.assertNotIn("changed-by-another-operator", output + error)

    @unittest.skipIf(os.name == "nt", "POSIX permissions are verified on the Linux deployment host")
    def test_prepared_files_are_owner_only(self):
        self.env.write_text(self.credentials(nacos="nacos"), encoding="utf-8")
        self.assertEqual(0, self.invoke("--write")[0])
        self.assertEqual(0o600, self.env.stat().st_mode & 0o777)
        self.assertEqual(0o700, self.package().stat().st_mode & 0o777)
        for path in self.package().iterdir():
            self.assertEqual(0o600, path.stat().st_mode & 0o777)


if __name__ == "__main__":
    unittest.main()
