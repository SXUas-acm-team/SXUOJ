#!/usr/bin/env python3
"""Inspect or add contest security tables; never rewrite existing business rows.

Uses the selected MySQL container's existing MYSQL_ROOT_PASSWORD. No credentials
are printed or placed in process arguments. Inspection is the default operation.
"""
import argparse
import json
import re
import subprocess
import sys


class MigrationError(RuntimeError):
    pass


def mysql(container, database, sql):
    command = [
        "docker", "exec", "-i", container, "sh", "-c",
        'export MYSQL_PWD="${MYSQL_ROOT_PASSWORD:?MYSQL_ROOT_PASSWORD is not set}"; '
        'exec mysql -uroot --batch --skip-column-names --default-character-set=utf8mb4 "$1"',
        "mysql", database,
    ]
    result = subprocess.run(command, input=sql, text=True, encoding="utf-8", capture_output=True)
    if result.returncode:
        raise MigrationError("MySQL operation failed: " + result.stderr.strip())
    return [line.split("\t") for line in result.stdout.splitlines() if line.strip()]


def definitions(cid_type):
    # Only a validated integer type obtained from information_schema is accepted.
    if not re.fullmatch(r"(?:bigint|int)(?:\(\d+\))?(?: unsigned)?", cid_type, re.I):
        raise MigrationError("Unsupported contest.id type: " + cid_type)
    return {
        "contest_access_policy": f"""CREATE TABLE `contest_access_policy` (
  `cid` {cid_type} NOT NULL,
  `release_at` DATETIME NULL DEFAULT NULL,
  `gmt_create` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`cid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
        "contest_protected_resource": f"""CREATE TABLE `contest_protected_resource` (
  `cid` {cid_type} NOT NULL,
  `resource_hash` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `resource_path` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  PRIMARY KEY (`cid`, `resource_hash`),
  KEY `idx_contest_resource_hash` (`resource_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    }


def inspect(query):
    version = query("SELECT VERSION();")[0][0]
    columns = query("SELECT COLUMN_TYPE FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='contest' AND COLUMN_NAME='id';")
    if len(columns) != 1:
        raise MigrationError("Selected database does not contain exactly one contest.id column.")
    cid_type = columns[0][0].lower()
    definitions(cid_type)
    state = {}
    for table in definitions(cid_type):
        state[table] = query("SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY FROM "
                             "information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() "
                             f"AND TABLE_NAME='{table}' ORDER BY ORDINAL_POSITION;")
        if state[table]:
            validate(table, state[table], cid_type)
    return version, cid_type, state


def validate(table, rows, cid_type):
    actual = {row[0]: row[1:] for row in rows}
    required = {"cid", "release_at", "gmt_create", "gmt_modified"} if table == "contest_access_policy" \
        else {"cid", "resource_hash", "resource_path"}
    if not required.issubset(actual):
        raise MigrationError(table + " exists with an incompatible structure; no automatic repair performed.")
    if actual["cid"][0].lower() != cid_type or actual["cid"][1] != "NO" or actual["cid"][2] != "PRI":
        raise MigrationError(table + ".cid must match contest.id and be a non-null primary key.")
    if table == "contest_access_policy":
        if actual["release_at"][0].lower() != "datetime" or actual["release_at"][1] != "YES":
            raise MigrationError("release_at must be a nullable DATETIME.")
    elif actual["resource_hash"][0].lower() != "char(64)" or actual["resource_hash"][2] != "PRI":
        raise MigrationError("resource_hash must be CHAR(64) and part of the primary key.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mysql-container", required=True)
    parser.add_argument("--database", default="hoj")
    parser.add_argument("--apply", action="store_true", help="Create missing tables after validating existing structures")
    parser.add_argument("--print-ddl", action="store_true")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", args.mysql_container):
        parser.error("Invalid container name")
    if not re.fullmatch(r"[A-Za-z0-9_]+", args.database):
        parser.error("Invalid database name")
    try:
        query = lambda sql: mysql(args.mysql_container, args.database, sql)
        version, cid_type, state = inspect(query)
        missing = [table for table, rows in state.items() if not rows]
        if args.print_ddl:
            for table in missing:
                print(definitions(cid_type)[table])
        if args.apply:
            for table in missing:
                query(definitions(cid_type)[table])
            _, _, state = inspect(query)
            missing = [table for table, rows in state.items() if not rows]
        print(json.dumps({"database": args.database, "mysql_version": version,
                          "contest_id_type": cid_type, "missing_tables": missing,
                          "ready": not missing, "applied": args.apply}, ensure_ascii=False))
        return 2 if missing else 0
    except (MigrationError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
