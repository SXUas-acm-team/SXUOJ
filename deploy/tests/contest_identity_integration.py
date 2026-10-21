"""Synthetic, loopback-only checks against the explicitly labelled backup test environment.

Positive provider responses are unit tested with an HTTP transport double. The SQL
fixtures below represent already verified synthetic people, never real identities.
No production API key or real student data is used by this script.
"""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import secrets
import uuid
from contest_integration import Suite, IntegrationError, sql_value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-url', required=True)
    parser.add_argument('--container-prefix', required=True)
    parser.add_argument('--database', default='hoj')
    parser.add_argument('--env-file', required=True, type=Path)
    parser.add_argument('--report', required=True, type=Path)
    args = parser.parse_args()
    s = Suite(args); s.guard()
    name = 'ctest_identity_' + uuid.uuid4().hex[:8]
    people, usernames = {}, {}
    browser_password = secrets.token_urlsafe(24)
    for actor, role in [('root', 'root'), ('admin', 'admin'), ('student', name), ('second', name)]:
        rows = s.db.query('SELECT id FROM role WHERE role=' + sql_value(role) + ' AND status=0 LIMIT 1')
        role_id = int(rows[0][0]) if rows else s.db.insert('role', {'role': role, 'description': name, 'status': 0})
        uid = uuid.uuid4().hex; username = 'idv_' + name.rsplit('_', 1)[-1] + '_' + actor
        s.db.insert('user_info', {'uuid': uid, 'username': username, 'nickname': username, 'realname': 'Self declared',
            'status': 0, 'password': hashlib.md5(browser_password.encode()).hexdigest()})
        s.db.insert('user_record', {'uid': uid}); s.db.insert('user_role', {'uid': uid, 'role_id': role_id})
        people[actor] = uid; usernames[actor] = username
    s.tokens = {actor: s.token(uid) for actor, uid in people.items()}
    login = s.success('admin_can_login_from_public_admin_login_page', '/api/admin/login', payload={
        'username': usernames['admin'], 'password': browser_password})
    s.check('admin_login_keeps_admin_role', 'admin' in login['roleList'] and 'root' not in login['roleList'])
    now = dt.datetime.now()
    start = now - dt.timedelta(hours=1); end = now + dt.timedelta(hours=1)
    cid = s.db.insert('contest', {'uid': people['root'], 'author': usernames['root'], 'title': name,
        'description': name, 'type': 0, 'auth': 0, 'status': 0, 'visible': True, 'seal_rank': False,
        'is_group': False, 'award_type': 0, 'allow_end_submit': False, 'open_rank': True, 'rank_show_name': 'realname',
        'start_time': start.strftime('%Y-%m-%d %H:%M:%S'), 'end_time': end.strftime('%Y-%m-%d %H:%M:%S'), 'duration': 7200})
    pid = s.db.insert('problem', {'problem_id': name.upper(), 'title': name, 'author': usernames['root'], 'type': 0,
        'auth': 1, 'judge_mode': 'default', 'judge_case_mode': 'default', 'time_limit': 1000, 'memory_limit': 128,
        'stack_limit': 128, 'description': 'Synthetic statement', 'input': 'Synthetic input', 'output': 'Synthetic output',
        'examples': '[]', 'is_remote': False, 'is_group': False, 'case_version': name})
    cpid = s.db.insert('contest_problem', {'cid': cid, 'pid': pid, 'display_id': 'A', 'display_title': name, 'color': '#123456'})
    language_id = int(s.db.scalar("SELECT id FROM language WHERE oj='ME' AND is_spj=0 ORDER BY id LIMIT 1"))
    s.db.insert('problem_language', {'pid': pid, 'lid': language_id})
    path = '/api/contest-identity/' + str(cid) + '/config'
    config = {'enabled': True, 'replaceRealname': True, 'starPrefixes': '0020',
        'apiUrl': 'https://sf-zt.gwy.fun/cloudxxsaacloud/v1/identities/verify-name', 'apiKey': 'integration-only-secret'}
    s.denied('anonymous_cannot_read_identity_configuration', path)
    s.denied('ordinary_user_cannot_manage_identity_configuration', path, 'student')
    s.success('global_admin_can_configure_another_owners_contest', path, 'admin', config, 'PUT')
    read = s.success('configuration_can_be_read_by_admin', path, 'admin')
    s.check('configuration_never_returns_api_key', 'apiKey' not in read and read['hasApiKey'] is True and 'integration-only-secret' not in json.dumps(read))
    encrypted = s.db.scalar('SELECT api_key FROM contest_identity_config WHERE cid=' + str(cid))
    s.check('api_key_encrypted_in_database', encrypted != config['apiKey'] and len(encrypted) > 40)
    info = s.success('identity_gate_metadata_available', '/api/get-contest-info?cid=' + str(cid), 'student')
    s.check('identity_gate_enabled_in_info', info['identityVerificationEnabled'] is True)
    access = s.success('unverified_access_description', '/api/get-contest-access?cid=' + str(cid), 'student')
    s.check('unverified_access_closed', access['canViewProblems'] is False and access['canSubmit'] is False and access['accessReason'] == 'IDENTITY_REQUIRED')
    s.denied('unverified_cannot_list_problems', '/api/get-contest-problem?cid=' + str(cid), 'student')
    s.denied('unverified_cannot_read_statement', '/api/get-contest-problem-details?cid=' + str(cid) + '&displayId=A', 'student')
    s.denied('malformed_identity_cannot_bind', '/api/contest-identity/verify', 'student', {'cid': cid, 'studentNumber': '00201234', 'name': ''})
    s.check('failed_verification_not_persisted', s.db.scalar('SELECT COUNT(*) FROM contest_identity_binding WHERE cid=' + str(cid)) == '0')
    # Bind only synthetic identities as fixtures, to exercise the real downstream access/rank paths.
    for actor, number, verified in [('student', '00201234', '测试核验姓名甲'), ('second', '20241234', '测试核验姓名乙')]:
        s.db.insert('contest_identity_binding', {'cid': cid, 'uid': people[actor], 'student_number': number, 'verified_name': verified,
            'verified_at': now.strftime('%Y-%m-%d %H:%M:%S')})
    for label, uid, number in [('duplicate_user_blocked', people['student'], 'another'), ('duplicate_student_blocked', people['admin'], '00201234')]:
        try:
            s.db.insert('contest_identity_binding', {'cid': cid, 'uid': uid, 'student_number': number, 'verified_name': '合成身份', 'verified_at': now.strftime('%Y-%m-%d %H:%M:%S')})
        except IntegrationError:
            s.check(label, True)
        else:
            s.check(label, False)
    access = s.success('verified_access_description', '/api/get-contest-access?cid=' + str(cid), 'student')
    s.check('verified_user_can_view_and_submit', access['canViewProblems'] is True and access['canSubmit'] is True)
    s.success('verified_can_list_problems', '/api/get-contest-problem?cid=' + str(cid), 'student')
    s.success('verified_can_read_statement', '/api/get-contest-problem-details?cid=' + str(cid) + '&displayId=A', 'student')
    s.denied('identity_cannot_be_rebound', '/api/contest-identity/verify', 'student', {'cid': cid, 'studentNumber': 'other', 'name': '其他姓名'})
    s.success('same_identity_verification_idempotent', '/api/contest-identity/verify', 'student', {'cid': cid, 'studentNumber': '00201234', 'name': '测试核验姓名甲'})
    for index, actor in enumerate(('student', 'second')):
        submit_time = start + dt.timedelta(minutes=index + 1)
        submit_id = s.db.insert('judge', {'pid': pid, 'uid': people[actor], 'username': usernames[actor], 'cid': cid,
            'cpid': cpid, 'display_pid': 'A', 'status': 0, 'submit_time': submit_time.strftime('%Y-%m-%d %H:%M:%S'),
            'share': False, 'code': '// synthetic fixture', 'language': 'C++', 'version': 0, 'length': 20})
        s.db.insert('contest_record', {'cid': cid, 'uid': people[actor], 'pid': pid, 'cpid': cpid, 'submit_id': submit_id,
            'display_id': 'A', 'status': 0, 'time': (index + 1) * 60, 'submit_time': submit_time.strftime('%Y-%m-%d %H:%M:%S'), 'checked': False})
    payload = {'cid': cid, 'currentPage': 1, 'limit': 30, 'forceRefresh': False, 'removeStar': False, 'containsEnd': False}
    page = s.success('verified_rank_available', '/api/get-contest-rank', 'student', payload)
    rows = {row['uid']: row for row in page['records']}
    s.check('rank_names_and_automatic_star', rows[people['student']]['realname'] == '测试核验姓名甲' and rows[people['student']]['rank'] == -1
        and rows[people['second']]['realname'] == '测试核验姓名乙' and rows[people['second']]['rank'] == 1)
    payload['removeStar'] = True
    page = s.success('star_filter_available', '/api/get-contest-rank', 'student', payload)
    s.check('automatic_star_removed_from_rank', all(row['uid'] != people['student'] for row in page['records']))
    config.update(apiKey='', starPrefixes='', replaceRealname=False)
    s.success('admin_can_update_options_without_key_reentry', path, 'admin', config, 'PUT')
    s.check('empty_key_preserves_ciphertext', s.db.scalar('SELECT api_key FROM contest_identity_config WHERE cid=' + str(cid)) == encrypted)
    payload['removeStar'] = False
    page = s.success('rank_updates_without_old_presentation', '/api/get-contest-rank', 'student', payload)
    s.check('original_name_restored_and_star_rule_removed', all(row['realname'] == 'Self declared' and row['rank'] != -1 for row in page['records']))
    system_status, system_body = s.request('/api/admin/user/get-user-list?currentPage=1&limit=10', 'admin')
    s.check('ordinary_admin_still_cannot_manage_system_accounts', system_status == 403 and system_body.get('data') is None)
    s.success('admin_can_manage_another_contest', '/api/admin/contest?cid=' + str(cid), 'admin')
    problem = s.success('admin_can_manage_another_problem', '/api/admin/problem?pid=' + str(pid), 'admin')
    problem_dto = {'problem': problem, 'samples': [], 'languages': [{'id': language_id}], 'tags': [], 'codeTemplates': []}
    problem_dto['problem']['description'] = 'Edited by ordinary administrator'
    problem_dto.update(isUploadTestCase=False, judgeMode='default', changeModeCode=False, changeJudgeCaseMode=False)
    s.success('admin_can_edit_another_problem', '/api/admin/problem', 'admin', problem_dto, 'PUT')
    s.check('edited_statement_persisted', s.db.scalar('SELECT description FROM problem WHERE id=' + str(pid)) == 'Edited by ordinary administrator')
    problem_dto['problem'].update(id=None, problemId=name.upper() + '_NEW', title=name + '_new', author=usernames['admin'])
    problem_dto.update(samples=[{'input': '1', 'output': '1', 'score': 100}], tags=[], codeTemplates=[])
    s.success('admin_can_create_problem', '/api/admin/problem', 'admin', problem_dto, 'POST')
    training_dto = {'training': {'title': name, 'description': 'Synthetic training', 'author': usernames['root'],
        'auth': 'Public', 'status': True, 'rank': 0, 'isGroup': False}, 'trainingCategory': {'name': name, 'color': '#123456'}}
    s.success('admin_can_create_training', '/api/admin/training', 'admin', training_dto, 'POST')
    tid = s.db.scalar('SELECT id FROM training WHERE BINARY title=BINARY ' + sql_value(name) + ' ORDER BY id DESC LIMIT 1')
    training_dto = s.success('admin_can_read_another_training', '/api/admin/training?tid=' + tid, 'admin')
    training_dto['training']['description'] = 'Edited synthetic training'
    s.success('admin_can_edit_another_training', '/api/admin/training', 'admin', training_dto, 'PUT')
    s.success('admin_can_delete_another_training', '/api/admin/training?tid=' + tid, 'admin', method='DELETE')
    category_id = s.db.insert('category', {'name': name})
    did = s.db.insert('discussion', {'uid': people['root'], 'author': usernames['root'], 'category_id': category_id,
        'title': name, 'content': 'Synthetic discussion', 'description': name, 'status': 0, 'top_priority': False})
    s.success('admin_can_moderate_another_discussion', '/api/admin/discussion', 'admin', {'id': did, 'content': 'Moderated synthetic discussion'}, 'PUT')
    s.success('admin_can_delete_another_discussion', '/api/admin/discussion', 'admin', [did], 'DELETE')
    s.success('admin_can_set_another_contest_color', '/api/admin/contest/contest-problem', 'admin', {'id': cpid, 'color': '#abcdef'}, 'PUT')
    admin_contest = s.success('admin_can_read_full_contest_settings', '/api/admin/contest?cid=' + str(cid), 'admin')
    admin_contest['description'] = 'Contest edited by ordinary administrator'
    s.success('admin_can_edit_another_contest', '/api/admin/contest', 'admin', admin_contest, 'PUT')
    admin_contest.update(id=None, title=name + '_delete', uid=people['root'], author=usernames['root'])
    s.success('admin_can_create_contest', '/api/admin/contest', 'admin', admin_contest, 'POST')
    deletion_cid = int(s.db.scalar('SELECT id FROM contest WHERE BINARY title=BINARY ' + sql_value(name + '_delete') + ' ORDER BY id DESC LIMIT 1'))
    config.update(enabled=False)
    s.success('delete_fixture_identity_configuration', '/api/contest-identity/' + str(deletion_cid) + '/config', 'admin', config, 'PUT')
    s.denied('admin_contest_deletion_still_requires_account_password', '/api/admin/contest', 'admin', {'cid': deletion_cid, 'password': ''}, 'DELETE')
    s.success('admin_can_delete_with_current_account_password', '/api/admin/contest', 'admin', {'cid': deletion_cid, 'password': browser_password}, 'DELETE')
    s.check('deleted_contest_identity_data_cleaned', s.db.scalar('SELECT COUNT(*) FROM contest_identity_config WHERE cid=' + str(deletion_cid)) == '0')
    # Private browser fixture contains only generated test credentials; never publish it.
    private_fixture = args.env_file.parent / 'identity-browser-fixture-v1.1.0.json'
    private_fixture.write_text(json.dumps({'username': usernames['admin'], 'password': browser_password, 'cid': cid, 'pid': pid}, ensure_ascii=False), encoding='utf-8')
    result = {'passed': len(s.assertions), 'failed': 0, 'checks': s.assertions,
              'positive_bindings': 'synthetic SQL fixtures; provider success and transactional writer covered by unit tests', 'production_modified': False}
    args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print('PASS: %s synthetic HTTP/database checks; production untouched.' % len(s.assertions))


if __name__ == '__main__':
    main()
