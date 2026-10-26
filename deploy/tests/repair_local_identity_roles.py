"""Repair missing submit grants on old synthetic identity fixtures, never production roles."""
import argparse
import json
from pathlib import Path
from contest_integration import Suite


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', required=True)
    parser.add_argument('--container-prefix', required=True)
    parser.add_argument('--database', default='hoj')
    parser.add_argument('--env-file', required=True, type=Path)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    suite = Suite(args)
    suite.guard()  # Enforces loopback URLs and the isolated backup environment labels.
    selection = """SELECT r.id, a.id FROM role r JOIN auth a ON a.permission='submit' AND a.status=0
        WHERE r.status=0 AND BINARY r.description=BINARY r.role
        AND r.role REGEXP '^ctest_identity_[0-9a-f]{8}$'
        AND a.id=(SELECT MIN(id) FROM auth WHERE permission='submit' AND status=0)
        AND NOT EXISTS (SELECT 1 FROM role_auth ra JOIN auth granted ON ra.auth_id=granted.id
                        WHERE ra.role_id=r.id AND granted.permission='submit')"""
    missing = suite.db.query(selection)
    if args.apply and missing:
        suite.db.query('INSERT INTO role_auth(role_id,auth_id) ' + selection)
    print(json.dumps({'missing_fixture_grants': len(missing), 'applied': args.apply,
                      'remaining': len(suite.db.query(selection)) if args.apply else len(missing)}))


if __name__ == '__main__':
    main()
