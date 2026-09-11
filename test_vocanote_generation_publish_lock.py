from __future__ import annotations
import json, tempfile, threading, time, unittest
from contextlib import contextmanager
from pathlib import Path
import vocanote_tombstone as tombstone
import vocanote_worker as worker
from vocanote_queue import enqueue_job, claim_next, connect

class GenerationPublishLockTest(unittest.TestCase):
    def test_stage_markers_reject_new_generation_and_audio(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); rid='11111111-1111-4111-8111-111111111111'
            audio1=root/'g1.m4a';audio1.write_bytes(b'g1-audio')
            job1={'recording_id':rid,'generation':1,'audio_path':str(audio1),'output_dir':str(root)}
            marker=worker.stage_generation_marker(job1,'stt');marker.write_text(json.dumps(worker.generation_identity(job1)))
            self.assertTrue(worker.stage_generation_ok(job1,'stt'))
            audio2=root/'g2.m4a';audio2.write_bytes(b'g2-audio')
            job2={'recording_id':rid,'generation':2,'audio_path':str(audio2),'output_dir':str(root)}
            self.assertFalse(worker.stage_generation_ok(job2,'stt'))
            self.assertFalse(worker.checkpoint_stt_ok(job2))
            self.assertFalse(worker.checkpoint_correction_ok(job2))
            self.assertFalse(worker.checkpoint_fast_ok(job2))
            self.assertFalse(worker.checkpoint_semantic_ok(job2))
            self.assertFalse(worker.checkpoint_render_ok(job2))

    def test_publish_and_generation_switch_are_serialized(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);db=root/'jobs.sqlite3';rid='11111111-1111-4111-8111-111111111111';audio=root/'audio.m4a';audio.write_bytes(b'audio');meta=root/'metadata.json';meta.write_text(json.dumps({'recording_id':rid}));status=root/'status.json';status.write_text(json.dumps({'recording_id':rid,'marker':'base'}))
            enqueue_job(job_id='j',recording_id=rid,audio_path=str(audio),metadata_path=str(meta),output_dir=str(root),generation=1,catalog_canonical=True,db_path=db)
            claimed=claim_next(worker.worker_id(),db_path=db);self.assertIsNotNone(claimed);job=dict(claimed or {});job['_db_path']=str(db)
            entered=threading.Event();allow=threading.Event();switched=threading.Event();errors=[];real_replace=tombstone.os.replace
            def slow_replace(src,dst): entered.set();allow.wait(5);return real_replace(src,dst)
            tombstone.os.replace=slow_replace
            def write():
                try: worker.guarded_write_json(job,status,{'marker':'g1'})
                except Exception as exc: errors.append(exc)
            def switch():
                try:
                    with connect(db) as c:
                        c.execute('BEGIN IMMEDIATE');c.execute("UPDATE safe_edit_recordings SET current_generation=2 WHERE recording_id=?",(rid,));c.execute('COMMIT')
                    switched.set()
                except Exception as exc: errors.append(exc)
            try:
                wt=threading.Thread(target=write);wt.start();self.assertTrue(entered.wait(3),repr(errors));st=threading.Thread(target=switch);st.start();time.sleep(.2);self.assertFalse(switched.is_set());allow.set();wt.join(5);st.join(5)
            finally: tombstone.os.replace=real_replace;allow.set()
            self.assertFalse(errors);self.assertTrue(switched.is_set());self.assertEqual(json.loads(status.read_text())['marker'],'g1')
            with connect(db) as c:
                refs=c.execute('''SELECT COUNT(*) n FROM safe_edit_file_references r JOIN safe_edit_files f ON f.file_id=r.file_id WHERE r.recording_id=? AND r.generation=1 AND r.role='STATUS' AND r.active=1 AND f.path=?''',(rid,str(status.resolve()))).fetchone()
            self.assertEqual(int(refs['n']),1)
            with self.assertRaises(Exception): worker.guarded_write_json(job,status,{'marker':'late-g1'})
            self.assertEqual(json.loads(status.read_text())['marker'],'g1')

    def test_registration_failure_does_not_publish_unreferenced_file(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);db=root/'jobs.sqlite3';rid='11111111-1111-4111-8111-111111111111';audio=root/'audio.m4a';audio.write_bytes(b'audio');meta=root/'metadata.json';meta.write_text(json.dumps({'recording_id':rid}));status=root/'status.json';status.write_text(json.dumps({'recording_id':rid,'marker':'base'}))
            enqueue_job(job_id='j',recording_id=rid,audio_path=str(audio),metadata_path=str(meta),output_dir=str(root),generation=1,catalog_canonical=True,db_path=db)
            claimed=claim_next(worker.worker_id(),db_path=db);self.assertIsNotNone(claimed);job=dict(claimed or {});job['_db_path']=str(db)
            real_register=worker._register_published_artifact
            worker._register_published_artifact=lambda *_: (_ for _ in ()).throw(RuntimeError('register failed'))
            try:
                with self.assertRaisesRegex(RuntimeError,'register failed'):
                    worker.guarded_write_json(job,status,{'recording_id':rid,'marker':'new'})
            finally:
                worker._register_published_artifact=real_register
            self.assertEqual(json.loads(status.read_text())['marker'],'base')
            with connect(db) as c:
                refs=c.execute('''SELECT COUNT(*) n FROM safe_edit_file_references r JOIN safe_edit_files f ON f.file_id=r.file_id WHERE r.recording_id=? AND r.role='STATUS' AND r.active=1 AND f.path=?''',(rid,str(status.resolve()))).fetchone()
            self.assertEqual(int(refs['n']),0)

    def test_commit_failure_restores_previous_file_and_rolls_back_reference(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);db=root/'jobs.sqlite3';rid='11111111-1111-4111-8111-111111111111';audio=root/'audio.m4a';audio.write_bytes(b'audio');meta=root/'metadata.json';meta.write_text(json.dumps({'recording_id':rid}));status=root/'status.json';status.write_text(json.dumps({'recording_id':rid,'marker':'base'}))
            enqueue_job(job_id='j',recording_id=rid,audio_path=str(audio),metadata_path=str(meta),output_dir=str(root),generation=1,catalog_canonical=True,db_path=db)
            claimed=claim_next(worker.worker_id(),db_path=db);self.assertIsNotNone(claimed);job=dict(claimed or {});job['_db_path']=str(db)
            real_lock=worker.generation_publish_lock
            @contextmanager
            def fail_after_publish(_job):
                with connect(db) as c:
                    c.execute('BEGIN IMMEDIATE')
                    try:
                        yield c
                        c.execute('ROLLBACK')
                        raise RuntimeError('commit failed')
                    except Exception:
                        try:c.execute('ROLLBACK')
                        except Exception:pass
                        raise
            worker.generation_publish_lock=fail_after_publish
            try:
                with self.assertRaisesRegex(RuntimeError,'commit failed'):
                    worker.guarded_write_json(job,status,{'recording_id':rid,'marker':'new'})
            finally:
                worker.generation_publish_lock=real_lock
            self.assertEqual(json.loads(status.read_text())['marker'],'base')
            with connect(db) as c:
                refs=c.execute('''SELECT COUNT(*) n FROM safe_edit_file_references r JOIN safe_edit_files f ON f.file_id=r.file_id WHERE r.recording_id=? AND r.role='STATUS' AND r.active=1 AND f.path=?''',(rid,str(status.resolve()))).fetchone()
            self.assertEqual(int(refs['n']),0)
            self.assertEqual(list(root.glob('.*.bak')),[])

    def test_same_db_tombstone_check_does_not_self_deadlock(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);db=root/'jobs.sqlite3';rid='11111111-1111-4111-8111-111111111111';audio=root/'audio.m4a';audio.write_bytes(b'audio');meta=root/'metadata.json';meta.write_text(json.dumps({'recording_id':rid}));status=root/'status.json';status.write_text(json.dumps({'recording_id':rid,'marker':'base'}))
            enqueue_job(job_id='j',recording_id=rid,audio_path=str(audio),metadata_path=str(meta),output_dir=str(root),generation=1,catalog_canonical=True,db_path=db)
            claimed=claim_next(worker.worker_id(),db_path=db);self.assertIsNotNone(claimed);job=dict(claimed or {});job['_db_path']=str(db)
            started=time.monotonic();worker.guarded_write_json(job,status,{'recording_id':rid,'marker':'same-db'})
            self.assertLess(time.monotonic()-started,2.0)
            self.assertEqual(json.loads(status.read_text())['marker'],'same-db')

if __name__=='__main__': unittest.main(verbosity=2)
