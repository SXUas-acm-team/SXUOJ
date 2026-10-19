import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("contest_policy_migrate", Path(__file__).parents[1] / "contest_policy_migrate.py")
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


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
            migration.validate("contest_access_policy", [["cid", "int(11)", "NO", "PRI"]], "bigint(20)")


if __name__ == "__main__":
    unittest.main()
