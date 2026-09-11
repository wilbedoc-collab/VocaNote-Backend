#!/usr/bin/env python3
from __future__ import annotations

import os
import signal
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from vocanote_process_runner import ProcessCancelled, run_job_process
from vocanote_queue import claim_next, enqueue_job, get_job, init_db


def process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def wait_process_gone(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not process_exists(pid):
            return True
        time.sleep(0.05)
    return not process_exists(pid)


class ProcessGroupRunnerTest(unittest.TestCase):
    def test_cancel_kills_child_and_grandchild_without_orphan(self) -> None:
        with tempfile.TemporaryDirectory(prefix='vocanote-runner-') as td:
            root = Path(td)
            db = root / 'jobs.sqlite3'
            rec = root / 'recording'
            rec.mkdir()
            audio = rec / 'audio.m4a'
            metadata = rec / 'metadata.json'
            audio.write_bytes(b'test')
            metadata.write_text('{}', encoding='utf-8')
            init_db(db)
            enqueue_job(
                job_id='job-1', recording_id='recording-1', audio_path=str(audio),
                metadata_path=str(metadata), output_dir=str(rec), db_path=db,
            )
            worker = 'test-worker'
            job = claim_next(worker_id=worker, db_path=db, lease_seconds=30)
            self.assertIsNotNone(job)
            assert job is not None

            child_script = root / 'child.py'
            grandchild_pid_path = root / 'grandchild.pid'
            child_script.write_text(
                "import os, signal, subprocess, sys, time\n"
                "g = subprocess.Popen([sys.executable, '-c', "
                "'import signal,time; signal.signal(signal.SIGINT, signal.SIG_IGN); time.sleep(120)'])\n"
                "open(sys.argv[1], 'w').write(str(g.pid))\n"
                "signal.signal(signal.SIGINT, lambda *_: None)\n"
                "time.sleep(120)\n",
                encoding='utf-8',
            )

            observed_child_pid: list[int] = []

            def request_cancel() -> None:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    current = get_job('job-1', db_path=db)
                    if current and current.get('process_pid') and grandchild_pid_path.exists():
                        observed_child_pid.append(int(current['process_pid']))
                        with sqlite3.connect(db) as conn:
                            conn.execute(
                                "UPDATE recording_jobs SET cancel_requested=1 WHERE job_id='job-1'"
                            )
                        return
                    time.sleep(0.02)
                self.fail('subprocess metadata was not persisted before cancellation')

            canceller = threading.Thread(target=request_cancel)
            canceller.start()
            with self.assertRaises(ProcessCancelled):
                run_job_process(
                    job=job,
                    worker_id=worker,
                    cmd=[sys.executable, str(child_script), str(grandchild_pid_path)],
                    stage='orphan_test', timeout=30, db_path=db, poll_interval=0.05,
                    cooperative_grace=0.1, terminate_grace=0.2,
                )
            canceller.join(timeout=2)
            self.assertFalse(canceller.is_alive())

            self.assertEqual(len(observed_child_pid), 1)
            child_pid = observed_child_pid[0]
            grandchild_pid = int(grandchild_pid_path.read_text(encoding='utf-8'))
            self.assertTrue(wait_process_gone(child_pid), f'child still exists: {child_pid}')
            self.assertTrue(wait_process_gone(grandchild_pid), f'grandchild orphaned: {grandchild_pid}')
            current = get_job('job-1', db_path=db)
            assert current is not None
            self.assertEqual(current['cancel_requested'], 1)
            self.assertIsNone(current['process_pid'])
            self.assertIsNone(current['process_group_id'])
            self.assertIsNone(current['process_stage'])
            self.assertEqual(current['generation'], 1)

    def test_success_clears_process_metadata_and_returns_output(self) -> None:
        with tempfile.TemporaryDirectory(prefix='vocanote-runner-ok-') as td:
            root = Path(td)
            db = root / 'jobs.sqlite3'
            rec = root / 'recording'
            rec.mkdir()
            audio = rec / 'audio.m4a'; audio.write_bytes(b'test')
            metadata = rec / 'metadata.json'; metadata.write_text('{}', encoding='utf-8')
            init_db(db)
            enqueue_job(job_id='job-2', recording_id='recording-2', audio_path=str(audio), metadata_path=str(metadata), output_dir=str(rec), db_path=db)
            worker = 'test-worker'
            job = claim_next(worker_id=worker, db_path=db, lease_seconds=30)
            assert job is not None
            result = run_job_process(
                job=job, worker_id=worker,
                cmd=[sys.executable, '-c', "print('ok'); import sys; print('err', file=sys.stderr)"],
                stage='success_test', timeout=10, db_path=db, poll_interval=0.05,
            )
            self.assertEqual(result.returncode, 0)
            self.assertEqual(result.stdout.strip(), 'ok')
            self.assertEqual(result.stderr.strip(), 'err')
            current = get_job('job-2', db_path=db)
            assert current is not None
            self.assertIsNone(current['process_pid'])
            self.assertIsNone(current['process_group_id'])
            self.assertIsNone(current['process_stage'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
