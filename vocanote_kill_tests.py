#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

SERVER = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server')
BASE = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱')
RECORDINGS = BASE / 'recordings'
PY = '/Users/ahnbot/.openclaw/workspace/hermes_eval/repo/venv/bin/python'
API = 'http://127.0.0.1:8793'
PLIST = '/Users/ahnbot/Library/LaunchAgents/com.re2o.vocanote-worker.plist'


def run(cmd: list[str] | str, timeout: int = 60, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=isinstance(cmd, str), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout, **kwargs)


def token() -> str:
    return (SERVER / '.upload_token').read_text().strip()


def launchctl(args: list[str], timeout: int = 60) -> subprocess.CompletedProcess:
    return run(['launchctl', *args], timeout=timeout)


def stop_launchd_worker() -> None:
    uid = os.getuid()
    launchctl(['bootout', f'gui/{uid}', PLIST], timeout=30)
    time.sleep(2)


def start_launchd_worker() -> None:
    uid = os.getuid()
    launchctl(['bootstrap', f'gui/{uid}', PLIST], timeout=30)
    launchctl(['kickstart', '-k', f'gui/{uid}/com.re2o.vocanote-worker'], timeout=30)
    time.sleep(2)


def ensure_launchd_worker() -> None:
    uid = os.getuid()
    p = launchctl(['print', f'gui/{uid}/com.re2o.vocanote-worker'], timeout=20)
    if p.returncode != 0:
        start_launchd_worker()


def status_of(rid: str) -> dict[str, Any]:
    p = RECORDINGS / rid / 'status.json'
    return json.loads(p.read_text()) if p.exists() else {'status': 'missing', 'step': 'missing'}


def job_and_events(rid: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    conn = sqlite3.connect(SERVER / 'jobs.sqlite3')
    conn.row_factory = sqlite3.Row
    job = dict(conn.execute('select * from recording_jobs where recording_id=?', (rid,)).fetchone())
    events = [dict(r) for r in conn.execute('select * from job_events where job_id=? order by event_id asc', (job['job_id'],))]
    return job, events


def upload(src: Path, run_dir: Path, name: str) -> str:
    tmp = run_dir / f'{name}.m4a'
    tmp.write_bytes(src.read_bytes())
    cmd = [
        'curl','-sS','-m','90','-X','POST',API+'/api/recordings',
        '-H',f'X-Upload-Token: {token()}',
        '-F',f'audio=@{tmp};type=audio/mp4;filename={name}.m4a',
        '-F','title=무제회의','-F','meeting_type=test','-F',f'memo=kill_test_{name}',
        '-F',f'recorded_at={datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '-F','duration_sec=60','-F','language=ko',
    ]
    p = run(cmd, timeout=100)
    data = json.loads(p.stdout)
    rid = data['recording_id']
    (run_dir / f'{name}_upload.json').write_text(json.dumps(data, ensure_ascii=False, indent=2))
    return rid


def start_manual_worker(run_dir: Path, name: str) -> subprocess.Popen:
    env = os.environ.copy()
    env['VOCANOTE_HEARTBEAT_INTERVAL'] = '5'
    env['VOCANOTE_LEASE_SECONDS'] = '20'
    env['PYTHONUNBUFFERED'] = '1'
    out = open(run_dir / f'{name}_manual_worker.out.log', 'w')
    err = open(run_dir / f'{name}_manual_worker.err.log', 'w')
    return subprocess.Popen([PY, str(SERVER / 'vocanote_worker.py'), '--once'], cwd=str(SERVER), stdout=out, stderr=err, env=env, start_new_session=True)


def kill_tree(pid: int) -> None:
    # Simulate a hard worker crash: kill the entire process group so the worker
    # cannot catch a child-process error and mark retry_wait itself.
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    time.sleep(1)


def wait_for_status(rid: str, target: str, timeout_sec: int = 240) -> list[dict[str, Any]]:
    hist = []
    start = time.time()
    while time.time() - start < timeout_sec:
        st = status_of(rid)
        item = {'t': round(time.time()-start, 1), 'status': st.get('status'), 'step': st.get('step')}
        hist.append(item)
        if st.get('status') == target:
            return hist
        if st.get('status') in {'completed','failed','cancelled'}:
            return hist
        time.sleep(2)
    return hist


def wait_completed(rid: str, timeout_sec: int = 600) -> list[dict[str, Any]]:
    hist=[]
    start=time.time()
    while time.time()-start < timeout_sec:
        st=status_of(rid)
        item={'t':round(time.time()-start,1),'status':st.get('status'),'step':st.get('step')}
        hist.append(item)
        if st.get('status') in {'completed','failed','cancelled'}:
            return hist
        time.sleep(5)
    return hist


def run_one(name: str, target_status: str, src: Path, run_dir: Path) -> dict[str, Any]:
    result: dict[str, Any] = {'name': name, 'target_status': target_status, 'started_at': datetime.now().isoformat(timespec='seconds')}
    stop_launchd_worker()
    rid = upload(src, run_dir, name)
    result['recording_id'] = rid
    proc = start_manual_worker(run_dir, name)
    result['manual_pid'] = proc.pid
    hist_to_target = wait_for_status(rid, target_status, timeout_sec=260)
    result['hist_to_target'] = hist_to_target
    reached = hist_to_target and hist_to_target[-1]['status'] == target_status
    result['target_reached'] = reached
    if not reached:
        kill_tree(proc.pid)
        start_launchd_worker()
        result['pass'] = False
        result['fail_reason'] = 'target status not reached before terminal/timeout'
        return result
    before_kill_job, before_kill_events = job_and_events(rid)
    result['before_kill_job'] = {k: before_kill_job.get(k) for k in ['status','step','attempts','claimed_by','heartbeat_at','lease_expires_at']}
    kill_tree(proc.pid)
    result['killed_at'] = datetime.now().isoformat(timespec='seconds')
    time.sleep(25)  # manual worker lease is 20 sec.
    start_launchd_worker()
    hist_after = wait_completed(rid, timeout_sec=700)
    result['hist_after_recovery'] = hist_after
    job, events = job_and_events(rid)
    result['final_job'] = {k: job.get(k) for k in ['status','step','attempts','claimed_by','heartbeat_at','lease_expires_at','error_code','result_json_path']}
    result['events'] = events
    claims = [e for e in events if e['event'] == 'claim']
    stale = [e for e in events if e['event'] == 'stale_recovery']
    skips = [e for e in events if e['event'] == 'checkpoint_skip']
    result['claim_count'] = len(claims)
    result['stale_recovery_count'] = len(stale)
    result['checkpoint_skips'] = [{'status': e['status'], 'step': e['step'], 'details': e['details']} for e in skips]
    d = RECORDINGS / rid
    result['files'] = {p.name: {'exists': p.exists(), 'bytes': p.stat().st_size if p.exists() else 0} for p in [d/'stt_raw.json', d/'transcript_raw.txt', d/'transcript_clean.json', d/'result.validated.json', d/'summary.md', d/'analysis.md']}
    # PASS criteria per stage.
    pass_basic = job['status'] == 'completed' and len(stale) >= 1 and len(claims) == 2 and job['attempts'] >= 1
    if target_status == 'correction_running':
        pass_basic = pass_basic and any('stt checkpoint valid' in str(e.get('details')) for e in skips)
    if target_status == 'semantic_running':
        pass_basic = pass_basic and any('stt checkpoint valid' in str(e.get('details')) for e in skips) and any('correction checkpoint valid' in str(e.get('details')) for e in skips)
    result['pass'] = bool(pass_basic)
    result['finished_at'] = datetime.now().isoformat(timespec='seconds')
    return result


def main() -> None:
    run_dir = Path(os.environ.get('VOCANOTE_OPERATIONAL_RUN_DIR') or (BASE / 'validation_runs' / ('operational_' + datetime.now().strftime('%Y%m%d_%H%M%S'))))
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / 'kill_tests_started.txt').write_text(datetime.now().isoformat(timespec='seconds'))
    src = BASE / '26081475세기증자의아주늘고지친피부세포가_original.m4a'
    tests = [
        ('kill_stt_running', 'stt_running'),
        ('kill_correction_running', 'correction_running'),
        ('kill_semantic_running', 'semantic_running'),
    ]
    results=[]
    try:
        for name, target in tests:
            item = run_one(name, target, src, run_dir)
            results.append(item)
            (run_dir / f'{name}.json').write_text(json.dumps(item, ensure_ascii=False, indent=2))
    finally:
        ensure_launchd_worker()
    report = {'run_dir': str(run_dir), 'results': results, 'pass': all(r.get('pass') for r in results)}
    (run_dir / 'kill_tests_report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
    # concise markdown
    lines=['# VocaNote Kill Test Report','', '| Test | Target | PASS | recording_id | claims | stale | checkpoints |', '|---|---|---:|---|---:|---:|---|']
    for r in results:
        lines.append(f"| {r['name']} | {r['target_status']} | {r.get('pass')} | `{r.get('recording_id')}` | {r.get('claim_count')} | {r.get('stale_recovery_count')} | {r.get('checkpoint_skips')} |")
    (run_dir / 'kill_tests_report.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps({'ok': True, 'run_dir': str(run_dir), 'pass': report['pass']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
