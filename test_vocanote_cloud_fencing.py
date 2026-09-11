from __future__ import annotations
import json, tempfile, unittest
from pathlib import Path
import vocanote_cloud as cloud

class _Call:
    def __init__(self, value): self.value=value
    def execute(self): return self.value
class _Files:
    def list(self, **_): return _Call({'files':[{'id':'remote'}]})
    def get(self, **_): return _Call({'id':'remote','name':'r.m4a','size':'4','md5Checksum':None})
class _Service:
    def files(self): return _Files()

class CloudFencingTest(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory();self.d=Path(self.t.name);(self.d/'audio.m4a').write_bytes(b'abcd');(self.d/'metadata.json').write_text(json.dumps({'recording_id':'r','audio_file':'audio.m4a'}))
        self.old_service,self.old_folder=cloud.drive_service,cloud.ensure_audio_folder
        cloud.drive_service=lambda:_Service();cloud.ensure_audio_folder=lambda *_:('folder','root/audio/2026/09')
    def tearDown(self):
        cloud.drive_service,cloud.ensure_audio_folder=self.old_service,self.old_folder;self.t.cleanup()
    def writer(self,path,payload): cloud.write_json(path,payload)
    def test_cataloged_preserves_canonical_and_uses_fenced_callback(self):
        writes=[]
        def writer(path,payload): writes.append(payload['audio_storage_state']);cloud.write_json(path,payload)
        out=cloud.upload_recording_audio_to_drive(self.d,write_json_fn=writer,assert_claim=lambda:True,preserve_local=True)
        self.assertTrue((self.d/'audio.m4a').exists());self.assertEqual(out['audio_storage_state'],'LOCAL_AND_CLOUD');self.assertEqual(writes,['CLOUD_UPLOAD_PENDING','CLOUD_UPLOADING','LOCAL_AND_CLOUD'])
    def test_authority_loss_prevents_unlink(self):
        calls={'n':0}
        def authority():
            calls['n']+=1
            if calls['n']>=2: raise RuntimeError('ownership_lost')
        with self.assertRaises(RuntimeError): cloud.upload_recording_audio_to_drive(self.d,write_json_fn=self.writer,assert_claim=authority,preserve_local=False)
        self.assertTrue((self.d/'audio.m4a').exists())
    def test_archive_never_directly_deletes_canonical(self):
        out=cloud.upload_recording_audio_to_drive(self.d,write_json_fn=self.writer,assert_claim=lambda:True,preserve_local=False)
        self.assertTrue((self.d/'audio.m4a').exists());self.assertEqual(out['audio_storage_state'],'LOCAL_AND_CLOUD')

if __name__=='__main__': unittest.main(verbosity=2)
