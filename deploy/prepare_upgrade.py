#!/usr/bin/env python3
"""Check upgrade credentials, or explicitly prepare a private, reversible rotation.

The default command is read-only. --write prepares SQL for a human to apply; it
never executes database writes or starts Spring in the selected backend image.
"""

import argparse
import base64
import binascii
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import tempfile


class PreparationError(Exception):
    """Messages must be fixed diagnostics, never external output or env values."""


PLACEHOLDERS = {"", "default", "hoj-secret-init", "no_judge_token", "hoj-judge-init", "hoj123456"}
FAMILIES = (("HOJ_JWT_SECRET", "JWT_TOKEN_SECRET"), ("HOJ_JUDGE_TOKEN", "JUDGE_TOKEN"))
BCRYPT = re.compile(r"\$2[aby]\$(?:0[4-9]|[12][0-9]|3[01])\$[./A-Za-z0-9]{53}\Z")
ASSIGNMENT = re.compile(r"(\s*(?:export\s+)?)([A-Za-z_][A-Za-z0-9_]*)(\s*=\s*)(.*)\Z")


def strong_jwt(value):
    try:
        return value not in PLACEHOLDERS and len(base64.b64decode(value + "=" * (-len(value) % 4), validate=True)) >= 64
    except (ValueError, TypeError, binascii.Error):
        return False


def strong_judge(value):
    return value not in PLACEHOLDERS and len(value) >= 32


def strong_nacos(value):
    return value not in PLACEHOLDERS and value != "nacos" and len(value) >= 12


def literal(raw):
    """Parse only unambiguous, one-line Compose literals; retain trailing comments."""
    body = raw.strip()
    suffix = raw[len(raw.rstrip()):]
    if body.startswith(("'", '"')):
        quote = body[0]
        end = 1
        while end < len(body):
            if body[end] == quote and (quote == "'" or body[end - 1] != "\\"):
                break
            end += 1
        if end == len(body):
            raise PreparationError("Environment contains unsupported quoting or multiline values.")
        tail = body[end + 1:]
        if tail and not re.fullmatch(r"\s+#.*", tail):
            raise PreparationError("Environment contains unsupported quoting or multiline values.")
        value = body[1:end]
        suffix = tail + suffix
        if quote == "'":
            if "\\" in value:
                raise PreparationError("Environment contains unsupported quoted escapes.")
            return value, suffix
        if re.search(r"\\(?![\\\"nrt])", value):
            raise PreparationError("Environment contains unsupported quoted escapes.")
        value = re.sub(r'\\([\\"nrt])', lambda m: {"n": "\n", "r": "\r", "t": "\t"}.get(m[1], m[1]), value)
    else:
        comment = re.search(r"\s+#", body)
        if comment:
            suffix = body[comment.start():] + suffix
            body = body[:comment.start()]
        value = body.rstrip()
    # Compose interpolates unquoted/double-quoted variables. Do not guess process
    # environment, defaults, required expansions or order-dependent references.
    if "$" in value.replace("$$", ""):
        raise PreparationError("Environment interpolation cannot safely be prepared automatically.")
    return value.replace("$$", "$"), suffix


class EnvDocument:
    def __init__(self, content):
        self.original = content
        self.bom = content.startswith(b"\xef\xbb\xbf")
        try:
            self.text = content.decode("utf-8-sig")
        except UnicodeError:
            raise PreparationError("Environment must be UTF-8 text.") from None
        if "\x00" in self.text:
            raise PreparationError("Environment must be plain text.")
        self.lines = self.text.splitlines(keepends=True)
        self.entries = {}
        self.values = {}
        for index, line in enumerate(self.lines):
            stripped = line.rstrip("\r\n")
            if not stripped.strip() or stripped.lstrip().startswith("#"):
                continue
            match = ASSIGNMENT.fullmatch(stripped)
            if not match:
                raise PreparationError("Environment contains unsupported assignments.")
            key = match[2]
            if key in self.entries:
                raise PreparationError("Environment has duplicate assignments; resolve them before preparation.")
            value, suffix = literal(match[4])
            newline = line[len(stripped):]
            self.entries[key] = (index, match[1] + key + match[3], suffix, newline)
            self.values[key] = value

    def render(self, changes):
        lines = list(self.lines)
        newline = "\r\n" if "\r\n" in self.text else "\n"
        for key, value in changes.items():
            # Generated/selected secrets must be representable without interpolation.
            if any(char in value for char in "'\r\n\x00"):
                raise PreparationError("A credential cannot safely be serialized to Compose environment syntax.")
            replacement = "'" + value + "'"
            if key in self.entries:
                index, prefix, suffix, ending = self.entries[key]
                lines[index] = prefix + replacement + suffix + ending
            else:
                if lines and not lines[-1].endswith(("\r", "\n")):
                    lines[-1] += newline
                lines.append(key + "=" + replacement + newline)
        return (("\ufeff" if self.bom else "") + "".join(lines)).encode("utf-8")


def family_state(values, keys, valid):
    populated = [values[key] for key in keys if values.get(key)]
    strong = {value for value in populated if valid(value)}
    return bool(populated and valid(populated[0])), len(strong) <= 1


def preflight(document):
    values = document.values
    jwt, jwt_alias = family_state(values, FAMILIES[0], strong_jwt)
    judge, judge_alias = family_state(values, FAMILIES[1], strong_judge)
    return {"env_literals_safe": True, "jwt_strong": jwt, "judge_token_strong": judge,
            "nacos_password_strong": strong_nacos(values.get("NACOS_PASSWORD", "")),
            "secret_aliases_unambiguous": jwt_alias and judge_alias,
            "nacos_enabled": values.get("NACOS_ENABLED", "true").lower() != "false"}


def family_changes(values, keys, valid):
    existing = [values[key] for key in keys if values.get(key)]
    strong = {value for value in existing if valid(value)}
    if len(strong) > 1:
        raise PreparationError("Strong credential aliases disagree; resolve them before preparation.")
    chosen = next(iter(strong)) if strong else base64.b64encode(secrets.token_bytes(64)).decode("ascii")
    return {key: chosen for key in keys if (key == keys[-1] or key in values) and values.get(key) != chosen}


def run_docker(arguments, stdin):
    try:
        result = subprocess.run(["docker", *arguments], input=stdin, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, encoding="utf-8", check=False, timeout=120)
    except (OSError, subprocess.SubprocessError):
        raise PreparationError("A required Docker operation failed; environment was not changed.") from None
    if result.returncode:
        raise PreparationError("A required Docker operation failed; environment was not changed.")
    return result.stdout


def nacos_hashes(args, values, password):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", args.mysql_container):
        raise PreparationError("Invalid MySQL container name.")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", args.nacos_database):
        raise PreparationError("Invalid Nacos database identifier.")
    if not args.backend_image or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_./:@-]*", args.backend_image):
        raise PreparationError("Nacos rotation requires a valid --backend-image containing the hash helper.")
    username = values.get("NACOS_USERNAME", "root")
    if not username or len(username.encode("utf-8")) > 255 or any(ord(c) < 32 for c in username):
        raise PreparationError("Invalid Nacos account name.")
    encoded_user = username.encode("utf-8").hex()
    where = "BINARY username=UNHEX('" + encoded_user + "')"
    query = "SELECT HEX(username),HEX(password) FROM `" + args.nacos_database + "`.`users` WHERE " + where + ";\n"
    # The existing root password stays inside its existing container environment.
    command = 'test -n "$MYSQL_ROOT_PASSWORD" || exit 65; MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql --batch --raw --skip-column-names -u root'
    response = run_docker(["exec", "-i", args.mysql_container, "sh", "-c", command], query)
    rows = response.strip().splitlines()
    try:
        fields = rows[0].split("\t") if len(rows) == 1 else []
        if len(fields) != 2 or bytes.fromhex(fields[0]).decode("utf-8") != username:
            raise ValueError()
        old_hash = bytes.fromhex(fields[1]).decode("ascii")
        if not BCRYPT.fullmatch(old_hash):
            raise ValueError()
    except (ValueError, UnicodeError):
        raise PreparationError("The selected Nacos account must uniquely exist with a BCrypt password.") from None
    new_hash = run_docker(["run", "--rm", "-i", "--network", "none", "--pull", "never", "--entrypoint", "java",
                           args.backend_image, "-cp", "/opt/hoj/tools:/opt/hoj/tools/hutool-all.jar",
                           "NacosPasswordHash"], password + "\n").strip()
    if not BCRYPT.fullmatch(new_hash):
        raise PreparationError("The offline password helper did not return a valid BCrypt hash.")
    return where, old_hash, new_hash


def update_sql(database, where, old_hash, new_hash):
    # Hash preconditions also prevent replay overwriting a password changed meanwhile.
    return ("-- Apply explicitly during the maintenance window. Expect exactly one updated row.\n"
            "START TRANSACTION;\nUPDATE `" + database + "`.`users` SET password=UNHEX('" + new_hash.encode("ascii").hex() + "')\n"
            "WHERE " + where + " AND BINARY password=UNHEX('" + old_hash.encode("ascii").hex() + "');\n"
            "SELECT ROW_COUNT() AS updated_existing_account;\nCOMMIT;\n")


def private_write(path, content):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        os.chmod(path, 0o600)
        output.write(content)
        output.flush()
        os.fsync(output.fileno())


def prepare(args, path, document):
    if path.is_symlink():
        raise PreparationError("Refusing to replace a symbolic-link environment file.")
    if not preflight(document)["nacos_enabled"]:
        raise PreparationError("Production upgrade requires Nacos to remain enabled.")
    changes = {}
    for keys, valid in zip(FAMILIES, (strong_jwt, strong_judge)):
        changes.update(family_changes(document.values, keys, valid))
    rotating_nacos = not strong_nacos(document.values.get("NACOS_PASSWORD", ""))
    sql = rollback = None
    if rotating_nacos:
        password = secrets.token_urlsafe(24)  # exactly 32 safe ASCII characters
        where, old_hash, new_hash = nacos_hashes(args, document.values, password)
        changes["NACOS_PASSWORD"] = password
        sql = update_sql(args.nacos_database, where, old_hash, new_hash)
        rollback = update_sql(args.nacos_database, where, new_hash, old_hash)
    if not changes:
        return {"prepared": True, "env_updated": False, "nacos_sql_required": False}
    rendered = document.render(changes)
    state = path.parent / ".sxuoj-upgrade"
    if state.is_symlink():
        raise PreparationError("Refusing a symbolic-link private state directory.")
    state.mkdir(mode=0o700, exist_ok=True)
    os.chmod(state, 0o700)
    package = Path(tempfile.mkdtemp(prefix="credentials-", dir=state))
    os.chmod(package, 0o700)
    private_write(package / "previous.env", document.original)
    if sql:
        private_write(package / "nacos-password.sql", sql.encode("utf-8"))
        private_write(package / "rollback-nacos-password.sql", rollback.encode("utf-8"))
    temporary = path.parent / ("." + path.name + ".upgrade-" + secrets.token_hex(12))
    try:
        private_write(temporary, rendered)
        if path.read_bytes() != document.original or path.is_symlink():
            raise PreparationError("Environment changed during preparation; nothing was replaced.")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return {"prepared": True, "env_updated": True, "nacos_sql_required": rotating_nacos,
            "private_package": str(package)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--backend-image")
    parser.add_argument("--mysql-container", default="hoj-mysql")
    parser.add_argument("--nacos-database", default="nacos")
    parser.add_argument("--write", action="store_true", help="Prepare private SQL/backup and atomically update env; never apply SQL")
    args = parser.parse_args(argv)
    try:
        path = Path(args.env_file).absolute()
        document = EnvDocument(path.read_bytes())
        report = prepare(args, path, document) if args.write else preflight(document)
        print(json.dumps(report, sort_keys=True))
        if args.write and report["nacos_sql_required"]:
            print("Apply the prepared Nacos SQL explicitly, then recreate Nacos and every JudgeServer client with the new environment before resuming submissions.")
        return 0
    except PreparationError as error:
        if not args.write:
            print(json.dumps({"env_literals_safe": False, "jwt_strong": False,
                              "judge_token_strong": False, "nacos_password_strong": False,
                              "secret_aliases_unambiguous": False, "nacos_enabled": False}, sort_keys=True))
        print("Preparation stopped: " + str(error), file=sys.stderr)
    except (OSError, ValueError):
        print("Preparation stopped: unable to read or safely prepare the selected environment.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
