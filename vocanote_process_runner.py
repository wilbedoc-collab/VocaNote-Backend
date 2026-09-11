#!/usr/bin/env python3
"""Owned subprocess runner with process-group cancellation and reaping."""
from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from vocanote_queue import (
    DB_PATH,
    OwnershipLost,
    add_job_event,
    assert_owner,
    connect,
    heartbeat_job,
    init_db,
    now_iso,
)


class ProcessCancelled(OwnershipLost):
    """The job was cancelled, superseded, or lost its ownership fence."""


def _claim_token(job: Mapping[str, Any]) -> str:
    token = str(job.get('claim_token') or '')
    if not token:
        raise OwnershipLost(f"missing claim_token job_id={job.get('job_id')}")
    return token


def _current_fence_reason(job: Mapping[str, Any], worker_id: str, db_path: Path) -> str:
    with connect(db_path) as conn:
        row = conn.execute(
            'SELECT status,cancel_requested,claimed_by,claim_token,generation FROM recording_jobs WHERE job_id=?',
            (job['job_id'],),
        ).fetchone()
        if row is None:
            return 'job_missing'
        if int(row['cancel_requested'] or 0):
            return 'cancel_requested'
        if row['claimed_by'] != worker_id or row['claim_token'] != _claim_token(job):
            return 'ownership_lost'
        if int(row['generation'] or 1) != int(job.get('generation') or 1):
            return 'generation_changed'
        current = conn.execute(
            'SELECT current_generation FROM safe_edit_recordings WHERE recording_id=?',
            (job['recording_id'],),
        ).fetchone()
        if current and int(current['current_generation']) != int(job.get('generation') or 1):
            return 'generation_superseded'
        return f"job_status={row['status']}"


def _persist_process(
    job: Mapping[str, Any], worker_id: str, pid: int, pgid: int, stage: str,
    db_path: Path,
) -> None:
    token = _claim_token(job)
    generation = int(job.get('generation') or 1)
    with connect(db_path) as conn:
        cur = conn.execute(
            '''
            UPDATE recording_jobs
            SET process_pid=?, process_group_id=?, process_stage=?, generation=?, updated_at=?
            WHERE job_id=? AND claimed_by=? AND claim_token=?
              AND generation=? AND COALESCE(cancel_requested,0)=0
              AND generation=COALESCE(
                  (SELECT current_generation FROM safe_edit_recordings
                   WHERE recording_id=recording_jobs.recording_id), generation
              )
            ''',
            (pid, pgid, stage, generation, now_iso(), job['job_id'], worker_id, token, generation),
        )
        if cur.rowcount != 1:
            raise ProcessCancelled(
                f"subprocess_start_fence_lost job_id={job['job_id']} stage={stage}"
            )


def _clear_process(job: Mapping[str, Any], pid: int, pgid: int, db_path: Path) -> None:
    """Clear only this exact invocation, even if cancellation cleared its claim."""
    with connect(db_path) as conn:
        conn.execute(
            '''
            UPDATE recording_jobs
            SET process_pid=NULL, process_group_id=NULL, process_stage=NULL, updated_at=?
            WHERE job_id=? AND generation=? AND process_pid=? AND process_group_id=?
            ''',
            (now_iso(), job['job_id'], int(job.get('generation') or 1), pid, pgid),
        )


def _group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _wait_group_exit(pgid: int, seconds: float, poll_interval: float) -> bool:
    deadline = time.monotonic() + max(0.0, seconds)
    while time.monotonic() < deadline:
        if not _group_exists(pgid):
            return True
        time.sleep(min(max(poll_interval, 0.01), max(0.0, deadline - time.monotonic())))
    return not _group_exists(pgid)


def _signal_group(pgid: int, sig: signal.Signals) -> None:
    try:
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError):
        # On macOS a group containing only an already-dead zombie can report
        # EPERM until its adopter reaps it; there is no live process to signal.
        pass


def _cancel_and_reap(
    proc: subprocess.Popen[bytes], pgid: int, *, poll_interval: float,
    cooperative_grace: float, terminate_grace: float,
) -> None:
    # SIGINT gives ffmpeg, Whisper, and Hermes a chance to stop cooperatively.
    _signal_group(pgid, signal.SIGINT)
    if not _wait_group_exit(pgid, cooperative_grace, poll_interval):
        _signal_group(pgid, signal.SIGTERM)
        if not _wait_group_exit(pgid, terminate_grace, poll_interval):
            _signal_group(pgid, signal.SIGKILL)
            _wait_group_exit(pgid, max(1.0, terminate_grace), poll_interval)
    # Always reap the direct child. A bounded final wait follows SIGKILL.
    try:
        proc.wait(timeout=max(1.0, terminate_grace))
    except subprocess.TimeoutExpired:
        _signal_group(pgid, signal.SIGKILL)
        proc.wait(timeout=max(1.0, terminate_grace))


def run_job_process(
    *,
    job: Mapping[str, Any],
    worker_id: str,
    cmd: Sequence[str | os.PathLike[str]],
    stage: str,
    timeout: float | None,
    db_path: Path = DB_PATH,
    env: Mapping[str, str] | None = None,
    cwd: str | os.PathLike[str] | None = None,
    poll_interval: float | None = None,
    cooperative_grace: float | None = None,
    terminate_grace: float | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run one owned command in a new session and cancel its whole process group.

    Ownership, cancellation, and generation are polled through the production
    recording_jobs row. The direct child is always waited/reaped before return.
    """
    db_path = Path(db_path)
    init_db(db_path)
    token = _claim_token(job)
    generation = int(job.get('generation') or 1)
    poll_interval = float(
        poll_interval if poll_interval is not None
        else os.environ.get('VOCANOTE_PROCESS_POLL_SECONDS', '1')
    )
    cooperative_grace = float(
        cooperative_grace if cooperative_grace is not None
        else os.environ.get('VOCANOTE_PROCESS_COOPERATIVE_GRACE_SECONDS', '2')
    )
    terminate_grace = float(
        terminate_grace if terminate_grace is not None
        else os.environ.get('VOCANOTE_PROCESS_TERM_GRACE_SECONDS', '8')
    )
    assert_owner(job['job_id'], worker_id=worker_id, claim_token=token, db_path=db_path)
    started = time.monotonic()

    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        proc = subprocess.Popen(
            [str(part) for part in cmd],
            stdout=stdout_file,
            stderr=stderr_file,
            env=dict(env) if env is not None else None,
            cwd=cwd,
            start_new_session=True,
        )
        pgid = os.getpgid(proc.pid)
        persisted = False
        cancel_reason: str | None = None
        timed_out = False
        try:
            try:
                _persist_process(job, worker_id, proc.pid, pgid, stage, db_path)
                persisted = True
            except BaseException:
                _cancel_and_reap(
                    proc, pgid, poll_interval=poll_interval,
                    cooperative_grace=cooperative_grace, terminate_grace=terminate_grace,
                )
                raise
            add_job_event(
                job_id=str(job['job_id']), recording_id=str(job['recording_id']),
                worker_id=worker_id, event='subprocess_started', status=job.get('status'),
                step=stage,
                details={'pid': proc.pid, 'pgid': pgid, 'stage': stage, 'generation': generation},
                db_path=db_path,
            )
            while proc.poll() is None:
                if timeout is not None and time.monotonic() - started >= timeout:
                    timed_out = True
                    cancel_reason = 'timeout'
                    break
                if not heartbeat_job(
                    str(job['job_id']), worker_id=worker_id, claim_token=token,
                    lease_seconds=max(1, int(max(poll_interval * 3, 30))), db_path=db_path,
                ):
                    cancel_reason = _current_fence_reason(job, worker_id, db_path)
                    break
                time.sleep(max(0.01, poll_interval))

            if cancel_reason is not None:
                _cancel_and_reap(
                    proc, pgid, poll_interval=poll_interval,
                    cooperative_grace=cooperative_grace, terminate_grace=terminate_grace,
                )
                add_job_event(
                    job_id=str(job['job_id']), recording_id=str(job['recording_id']),
                    worker_id=worker_id, event='subprocess_cancelled', status=None, step=stage,
                    details={'pid': proc.pid, 'pgid': pgid, 'stage': stage,
                             'generation': generation, 'reason': cancel_reason},
                    db_path=db_path,
                )
                if timed_out:
                    raise subprocess.TimeoutExpired([str(x) for x in cmd], float(timeout or 0))
                raise ProcessCancelled(
                    f"subprocess_cancelled job_id={job['job_id']} stage={stage} reason={cancel_reason}"
                )

            proc.wait()
            # A direct child can exit while a spawned descendant keeps the
            # process group alive. Reap/terminate the remainder before any
            # successful return so no Hermes/Whisper helper becomes orphaned.
            if _group_exists(pgid):
                _cancel_and_reap(
                    proc, pgid, poll_interval=poll_interval,
                    cooperative_grace=0.0, terminate_grace=terminate_grace,
                )
            # A natural exit is not sufficient if the owner/generation fence changed
            # in the same polling interval.
            assert_owner(job['job_id'], worker_id=worker_id, claim_token=token, db_path=db_path)
            with connect(db_path) as conn:
                row = conn.execute(
                    'SELECT generation FROM recording_jobs WHERE job_id=?', (job['job_id'],)
                ).fetchone()
                if row is None or int(row['generation']) != generation:
                    raise ProcessCancelled(
                        f"subprocess_return_generation_lost job_id={job['job_id']} stage={stage}"
                    )
            stdout_file.seek(0); stderr_file.seek(0)
            stdout = stdout_file.read().decode('utf-8', errors='replace')
            stderr = stderr_file.read().decode('utf-8', errors='replace')
            add_job_event(
                job_id=str(job['job_id']), recording_id=str(job['recording_id']),
                worker_id=worker_id, event='subprocess_reaped', status=job.get('status'),
                step=stage,
                details={'pid': proc.pid, 'pgid': pgid, 'stage': stage,
                         'generation': generation, 'returncode': proc.returncode},
                db_path=db_path,
            )
            return subprocess.CompletedProcess(
                [str(x) for x in cmd], int(proc.returncode or 0), stdout, stderr
            )
        finally:
            if proc.poll() is None:
                _cancel_and_reap(
                    proc, pgid, poll_interval=poll_interval,
                    cooperative_grace=cooperative_grace, terminate_grace=terminate_grace,
                )
            if persisted:
                _clear_process(job, proc.pid, pgid, db_path)
