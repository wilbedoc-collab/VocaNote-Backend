from __future__ import annotations
import tempfile, unittest
from pathlib import Path
from vocanote_safe_edit import SafeEditStore

class PurgeProofTest(unittest.TestCase):
    def test_generation_switch_carries_shared_workspace_refs(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); rid='11111111-1111-4111-8111-111111111111'
            audio=root/'audio.m4a';audio.write_bytes(b'audio');metadata=root/'metadata.json';metadata.write_text('{}')
            store=SafeEditStore(root/'jobs.sqlite3',foundation_enabled=True,destructive_enabled=False)
            store.register_recording(rid,audio);store.register_generation_artifacts(rid,1,[('METADATA',metadata)])
            with store.connect() as conn:
                conn.execute('BEGIN IMMEDIATE');purge_id=store._purge_intent(conn,rid,1,carry_to_generation=2);conn.execute('COMMIT')
            result=store.purge_server()
            self.assertEqual(result,{'purged':1,'failed':0});self.assertFalse(audio.exists());self.assertTrue(metadata.exists())
            self.assertEqual(store.live_reference_count_for_path(metadata),1)
            purge=store.get_purge(purge_id);self.assertEqual((purge or {})['server_status'],'SERVER_PURGED')

    def test_silent_delete_failure_cannot_ack(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); audio=root/'audio.m4a'; audio.write_bytes(b'audio')
            store=SafeEditStore(root/'jobs.sqlite3',foundation_enabled=True,destructive_enabled=False)
            store.register_recording('11111111-1111-4111-8111-111111111111',audio)
            with store.connect() as conn:
                conn.execute('BEGIN IMMEDIATE')
                purge_id=store._purge_intent(conn,'11111111-1111-4111-8111-111111111111',1)
                conn.execute('COMMIT')
            first=store.purge_server(unlink_fn=lambda _path: None)
            self.assertEqual(first,{'purged':0,'failed':1})
            self.assertTrue(audio.exists())
            pending=store.get_purge(purge_id); self.assertIsNotNone(pending)
            self.assertEqual((pending or {})['server_status'],'PURGE_PENDING')
            second=store.purge_server()
            self.assertEqual(second,{'purged':1,'failed':0})
            self.assertFalse(audio.exists())
            complete=store.get_purge(purge_id); self.assertIsNotNone(complete)
            self.assertEqual((complete or {})['server_status'],'SERVER_PURGED')

    def test_old_canonical_cannot_be_reclassified_by_new_generation_worker_scan(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);rid='11111111-1111-4111-8111-111111111111';old=root/'audio.m4a';old.write_bytes(b'old');new=root/'generations'/'g000002'/'audio.m4a';new.parent.mkdir(parents=True);new.write_bytes(b'new')
            store=SafeEditStore(root/'jobs.sqlite3',foundation_enabled=True,destructive_enabled=False);store.register_recording(rid,old)
            with store.connect() as conn:
                conn.execute('BEGIN IMMEDIATE');purge_id=store._purge_intent(conn,rid,1,carry_to_generation=2);new_fid=store._file(conn,new);ts='2026-09-08T00:00:00+00:00';conn.execute('INSERT INTO safe_edit_generations VALUES(?,?,?,?,0,?)',(rid,2,new_fid,'PROCESSING',ts));conn.execute('INSERT INTO safe_edit_file_references VALUES(?,?,?,?,?,?,?)',('newcanonical',new_fid,rid,2,'CANONICAL_AUDIO',1,ts));conn.execute('UPDATE safe_edit_recordings SET current_generation=2,canonical_file_id=? WHERE recording_id=?',(new_fid,rid));conn.execute('COMMIT')
            store.register_artifacts_if_cataloged(rid,2,[('JOB_ARTIFACT',old)])
            with store.connect() as conn:
                old_live=conn.execute('''SELECT COUNT(*) n FROM safe_edit_file_references r JOIN safe_edit_files f ON f.file_id=r.file_id WHERE f.path=? AND r.active=1''',(str(old.resolve()),)).fetchone()['n']
            self.assertEqual(int(old_live),0)
            self.assertEqual(store.purge_server(),{'purged':1,'failed':0});self.assertFalse(old.exists());self.assertTrue(new.exists());purge=store.get_purge(purge_id);self.assertIsNotNone(purge);self.assertEqual((purge or {})['server_status'],'SERVER_PURGED')

if __name__=='__main__': unittest.main(verbosity=2)
