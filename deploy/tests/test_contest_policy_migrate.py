import importlib.util
import copy
import io
from pathlib import Path
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("contest_policy_migrate", Path(__file__).parents[1] / "contest_policy_migrate.py")
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


def schemas(version="8.0", cid_type="bigint"):
    def column(name, kind, nullable="NO", key="", default=None, extra="", charset="", collation=""):
        return [name, kind, nullable, key, "1" if default is None else "0", default or "", extra, charset, collation]
    generated = "DEFAULT_GENERATED" if version == "8.0" else ""
    timestamp = "current_timestamp()" if version == "8.0" else "CURRENT_TIMESTAMP"
    policy = [column("cid", cid_type, key="PRI"), column("release_at", "datetime", "YES"),
              column("gmt_create", "datetime", default=timestamp, extra=generated),
              column("gmt_modified", "datetime", default=timestamp,
                     extra=(generated + " on update " + timestamp).strip())]
    resources = [column("cid", cid_type, key="PRI"),
                 column("resource_hash", "char(64)", key="PRI", charset="ascii", collation="ascii_bin"),
                 column("resource_path", "text", charset="utf8mb4", collation="utf8mb4_bin")]
    return {"contest_access_policy": policy, "contest_protected_resource": resources}


def indexes():
    return {
        "contest_access_policy": [["PRIMARY", "0", "1", "cid", "", "BTREE"]],
        "contest_protected_resource": [
            ["PRIMARY", "0", "1", "cid", "", "BTREE"],
            ["PRIMARY", "0", "2", "resource_hash", "", "BTREE"],
            ["idx_contest_resource_hash", "1", "1", "resource_hash", "", "BTREE"]],
    }


def fake_query(columns, keys, calls, cid_type="bigint", version="8.0.43"):
    def query(sql):
        calls.append(sql)
        if sql == "SELECT VERSION();":
            return [[version]]
        if "SELECT COLUMN_TYPE FROM" in sql:
            return [[cid_type]]
        for table in columns:
            if "TABLE_NAME='" + table + "'" in sql:
                return keys[table] if "information_schema.STATISTICS" in sql else columns[table]
        return []
    return query


class ContestMigrationTests(unittest.TestCase):
    def test_type_matches_signed_and_unsigned_production_identifiers(self):
        for column_type in ("bigint(20)", "bigint unsigned", "int(11) unsigned"):
            ddl = migration.definitions(column_type)
            self.assertEqual(len(ddl), 2)
            for statement in ddl.values():
                self.assertIn("`cid` " + column_type + " NOT NULL", statement)
                self.assertNotIn("ALTER TABLE", statement)
                self.assertNotIn("DROP TABLE", statement)

    def test_untrusted_type_is_rejected_before_sql_can_be_constructed(self):
        for column_type in ("bigint; DROP TABLE contest", "varchar(255)", "bigint unsigned /*test*/"):
            with self.assertRaises(migration.MigrationError):
                migration.definitions(column_type)

    def test_inspection_performs_only_selects_and_reports_missing_tables(self):
        calls = []
        def query(sql):
            calls.append(sql)
            if sql == "SELECT VERSION();":
                return [["5.7.44"]]
            if "COLUMN_TYPE FROM" in sql:
                return [["bigint(20)"]]
            return []
        version, cid_type, state = migration.inspect(query)
        self.assertEqual(version, "5.7.44")
        self.assertEqual(cid_type, "bigint(20)")
        self.assertTrue(all(not rows for rows in state.values()))
        self.assertTrue(all(sql.startswith("SELECT ") for sql in calls))

    def test_existing_incompatible_schema_is_rejected_without_repair(self):
        with self.assertRaises(migration.MigrationError):
            migration.validate("contest_access_policy", [["cid", "int(11)", "NO", "PRI"]], "bigint(20)", [])

    def test_existing_57_and_80_metadata_are_compatible_including_integer_width(self):
        for version, width in (("5.7", "bigint(20) unsigned"), ("8.0", "bigint unsigned")):
            with self.subTest(version=version):
                columns = schemas(version, width)
                keys = indexes()
                for table, rows in columns.items():
                    migration.validate(table, rows, "bigint unsigned", keys[table])
                calls = []
                _, _, state = migration.inspect(fake_query(columns, keys, calls, "bigint(20) unsigned"))
                self.assertTrue(all(state.values()))
                self.assertTrue(all(sql.startswith("SELECT ") for sql in calls))

    def test_manual_release_requires_sql_null_default(self):
        for default in ("CURRENT_TIMESTAMP", "current_timestamp()", "2026-01-01 00:00:00", "NULL"):
            rows = schemas()["contest_access_policy"]
            rows[1][4:6] = ["0", default]
            with self.subTest(default=default), self.assertRaises(migration.MigrationError):
                migration.validate("contest_access_policy", rows, "bigint", indexes()["contest_access_policy"])

    def test_explicit_zero_precision_timestamp_defaults_are_semantically_equivalent(self):
        table = "contest_access_policy"
        rows = schemas()[table]
        for row in rows[1:]:
            row[1] = "datetime(0)"
        for row in rows[2:]:
            row[5] = "CURRENT_TIMESTAMP(0)"
        rows[3][6] = "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(0)"
        migration.validate(table, rows, "bigint(20)", indexes()[table])

    def test_policy_requires_exact_single_cid_primary_key(self):
        table = "contest_access_policy"
        extra_key = ["PRIMARY", "0", "2", "gmt_create", "", "BTREE"]
        for keys in ([], indexes()[table] + [extra_key],
                     [["PRIMARY", "0", "1", "gmt_create", "", "BTREE"]]):
            with self.subTest(keys=keys), self.assertRaises(migration.MigrationError):
                migration.validate(table, schemas()[table], "bigint", keys)

    def test_resource_primary_key_column_order_and_full_columns_are_required(self):
        table = "contest_protected_resource"
        for mutation in ("reversed", "prefix", "extra"):
            keys = indexes()[table]
            if mutation == "reversed":
                keys[0][3], keys[1][3] = keys[1][3], keys[0][3]
            elif mutation == "prefix":
                keys[1][4] = "32"
            else:
                keys.insert(2, ["PRIMARY", "0", "3", "resource_path", "16", "BTREE"])
            with self.subTest(mutation=mutation), self.assertRaises(migration.MigrationError):
                migration.validate(table, schemas()[table], "bigint", keys)

    def test_resource_hash_requires_full_nonunique_secondary_index(self):
        table = "contest_protected_resource"
        for mutation in ("missing", "prefix", "wrong_column", "unique"):
            keys = indexes()[table]
            if mutation == "missing":
                keys.pop()
            elif mutation == "prefix":
                keys[-1][4] = "32"
            elif mutation == "wrong_column":
                keys[-1][3] = "cid"
            else:
                keys[-1][1] = "0"
            with self.subTest(mutation=mutation), self.assertRaises(migration.MigrationError):
                migration.validate(table, schemas()[table], "bigint", keys)

    def test_resource_column_drift_cannot_silently_truncate_or_change_path_matching(self):
        table = "contest_protected_resource"
        for row, field, value in ((2, 1, "varchar(64)"), (2, 2, "YES"), (2, 7, "latin1"),
                                  (2, 8, "utf8mb4_general_ci"), (1, 1, "varchar(64)"),
                                  (1, 2, "YES"), (1, 7, "utf8mb4"), (1, 8, "ascii_general_ci")):
            rows = schemas()[table]
            rows[row][field] = value
            with self.subTest(row=row, field=field, value=value), self.assertRaises(migration.MigrationError):
                migration.validate(table, rows, "bigint", indexes()[table])

    def test_audit_columns_require_datetime_timestamp_defaults_and_update_rule(self):
        table = "contest_access_policy"
        for row, field, value in ((2, 1, "int"), (2, 2, "YES"), (2, 4, "1"),
                                  (3, 5, "2026-01-01 00:00:00"), (3, 6, "DEFAULT_GENERATED")):
            rows = schemas()[table]
            rows[row][field] = value
            with self.subTest(row=row, field=field, value=value), self.assertRaises(migration.MigrationError):
                migration.validate(table, rows, "bigint", indexes()[table])

    def test_extra_required_column_or_unique_index_is_rejected(self):
        table = "contest_protected_resource"
        rows = schemas()[table]
        rows.append(["required_extra", "int", "NO", "", "1", "", "", "", ""])
        with self.assertRaises(migration.MigrationError):
            migration.validate(table, rows, "bigint", indexes()[table])
        keys = indexes()[table] + [["unique_cid", "0", "1", "cid", "", "BTREE"]]
        with self.assertRaises(migration.MigrationError):
            migration.validate(table, schemas()[table], "bigint", keys)

    def test_unsignedness_remains_significant_when_display_width_is_ignored(self):
        with self.assertRaises(migration.MigrationError):
            migration.validate("contest_access_policy", schemas(cid_type="bigint(20)")["contest_access_policy"],
                               "bigint unsigned", indexes()["contest_access_policy"])

    def test_apply_stops_before_creating_other_missing_table_when_existing_table_has_drift(self):
        columns = copy.deepcopy(schemas())
        columns["contest_access_policy"][1][4:6] = ["0", "CURRENT_TIMESTAMP"]
        columns["contest_protected_resource"] = []
        calls = []
        query = fake_query(columns, indexes(), calls)
        with mock.patch.object(migration, "mysql", side_effect=lambda container, database, sql: query(sql)), \
                mock.patch("sys.stderr", new_callable=io.StringIO) as errors, \
                mock.patch("sys.stdout", new_callable=io.StringIO) as output:
            result = migration.main(["--mysql-container", "isolated-mysql", "--apply"])
        self.assertEqual(result, 1)
        self.assertIn("release_at", errors.getvalue())
        self.assertNotIn('"ready": true', output.getvalue())
        self.assertTrue(all(sql.startswith("SELECT ") for sql in calls))


if __name__ == "__main__":
    unittest.main()
