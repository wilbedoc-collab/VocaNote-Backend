#!/usr/bin/env python3
"""Phase88A safe-edit foundation.

No production audio is edited by this module. Filesystem deletion is only
performed by an explicit purge worker after a committed canonical switch.
The production destructive feature flag defaults to OFF.
"""
from __future__ import annotations

import json
import hashlib
import os
import shutil
import signal
import sqlite3
import subprocess
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

FOUNDATION_ENV = 'VOCANOTE_SAFE_EDIT_FOUNDATION_ENABLED'
DESTRUCTIVE_ENV = 'VOCANOTE_TRIM_SPLIT_ENABLED'
DEFAULT_GRACE_SECONDS = int(os.environ.get('VOCANOTE_EDIT_GRACE_SECONDS', '5'))
REFERENCE_ROLES = {
    'CANONICAL_AUDIO','RAW_STT','TRANSCRIPT_RAW','CLEAN_TRANSCRIPT','SEGMENTS',
    'CORRECTION','FAST','SEMANTIC','FINAL','SUMMARY','KEYPOINTS','TITLE',
    'CHECKPOINT','JOB_ARTIFACT','STATUS','METADATA','WAVEFORM_CACHE','PLAYBACK_POSITION','UI_CACHE','CACHE','TEMP',
}

class SafeEditError(RuntimeError): pass
class StaleGeneration(SafeEditError): pass

def utc_now() -> datetime: return datetime.now(timezone.utc)
def iso(dt: datetime) -> str: return dt.astimezone(timezone.utc).isoformat(timespec='microseconds')
def parse_iso(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace('Z','+00:00')) if value else None

def _probe_bin(name: str) -> str:
    homebrew=Path('/opt/homebrew/bin')/name
    return str(homebrew) if homebrew.exists() else name

def validate_audio_file(path: str | Path, expected_duration: float | None = None, tolerance_sec: float = 0.5) -> dict[str, Any]:
    p=Path(path); errors=[]; data:dict[str,Any]={}
    if not p.is_file(): return {'ok':False,'errors':['file_missing'],'path':str(p),'timeline_start_sec':0.0}
    if p.stat().st_size<=0: return {'ok':False,'errors':['file_empty'],'path':str(p),'timeline_start_sec':0.0}
    try:
        cp=subprocess.run([_probe_bin('ffprobe'),'-v','error','-show_entries','format=duration,format_name:stream=codec_name,codec_type,sample_rate,channels','-of','json',str(p)],capture_output=True,text=True,timeout=30)
        if cp.returncode: errors.append('ffprobe_failed')
        else: data=json.loads(cp.stdout or '{}')
    except Exception: errors.append('ffprobe_failed')
    duration=None; codec=None; container=None
    try: duration=float((data.get('format') or {}).get('duration')); container=(data.get('format') or {}).get('format_name')
    except Exception: errors.append('duration_missing')
    streams=[x for x in data.get('streams',[]) if x.get('codec_type')=='audio']
    if streams: codec=streams[0].get('codec_name')
    else: errors.append('audio_stream_missing')
    if not codec: errors.append('codec_unrecognized')
    if duration is not None and duration<=0: errors.append('duration_nonpositive')
    if expected_duration is not None and duration is not None and abs(duration-float(expected_duration))>float(tolerance_sec): errors.append('duration_out_of_tolerance')
    try:
        cp=subprocess.run([_probe_bin('ffmpeg'),'-v','error','-xerror','-i',str(p),'-f','null','-'],capture_output=True,text=True,timeout=60)
        if cp.returncode: errors.append('decode_probe_failed')
    except Exception: errors.append('decode_probe_failed')
    return {'ok':not errors,'errors':errors,'path':str(p),'bytes':p.stat().st_size,'duration_sec':duration,'container':container,'codec':codec,'timeline_start_sec':0.0,'decode_probe':'PASS' if 'decode_probe_failed' not in errors else 'FAIL'}

class SafeEditStore:
    def __init__(self, db_path: str | Path, *, now_fn: Callable[[],datetime]=utc_now, foundation_enabled: bool | None=None, destructive_enabled: bool | None=None):
        self.db_path=Path(db_path);self.now_fn=now_fn
        self.foundation_enabled=(os.environ.get(FOUNDATION_ENV,'1')=='1') if foundation_enabled is None else foundation_enabled
        self.destructive_enabled=(os.environ.get(DESTRUCTIVE_ENV,'0')=='1') if destructive_enabled is None else destructive_enabled
    def now(self): return self.now_fn().astimezone(timezone.utc)
    @contextmanager
    def connect(self):
        self.db_path.parent.mkdir(parents=True,exist_ok=True)
        c=sqlite3.connect(str(self.db_path),timeout=30,isolation_level=None);c.row_factory=sqlite3.Row
        try:c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA foreign_keys=ON');yield c
        finally:c.close()
    def init_schema(self):
        with self.connect() as c:
            c.executescript('''
CREATE TABLE IF NOT EXISTS safe_edit_recordings(
 recording_id TEXT PRIMARY KEY,current_generation INTEGER NOT NULL,canonical_file_id TEXT NOT NULL,state TEXT NOT NULL,
 pre_edit_state TEXT,edit_grace_deadline TEXT,edit_lease_token TEXT,edit_lease_owner TEXT,edit_lease_acquired_at TEXT,
 edit_lease_expires_at TEXT,edit_lease_heartbeat TEXT,parent_recording_id TEXT,visible INTEGER NOT NULL DEFAULT 1,
 created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS safe_edit_generations(
 recording_id TEXT NOT NULL,generation INTEGER NOT NULL,audio_file_id TEXT NOT NULL,state TEXT NOT NULL,superseded INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL,PRIMARY KEY(recording_id,generation));
CREATE TABLE IF NOT EXISTS safe_edit_files(
 file_id TEXT PRIMARY KEY,path TEXT NOT NULL UNIQUE,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS safe_edit_file_references(
 reference_id TEXT PRIMARY KEY,file_id TEXT NOT NULL,recording_id TEXT NOT NULL,generation INTEGER NOT NULL,role TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL,
 UNIQUE(file_id,recording_id,generation,role));
CREATE INDEX IF NOT EXISTS idx_safe_refs_file_active ON safe_edit_file_references(file_id,active);
CREATE TABLE IF NOT EXISTS safe_edit_staged_candidates(
 op_id TEXT NOT NULL,recording_id TEXT NOT NULL,role TEXT NOT NULL,path TEXT NOT NULL,validation_json TEXT NOT NULL,validated INTEGER NOT NULL,created_at TEXT NOT NULL,
 PRIMARY KEY(op_id,role));
CREATE TABLE IF NOT EXISTS safe_edit_pipeline_jobs(
 job_id TEXT PRIMARY KEY,recording_id TEXT NOT NULL,generation INTEGER NOT NULL,status TEXT NOT NULL,cancel_requested INTEGER NOT NULL DEFAULT 0,
 claimed_by TEXT,claim_token TEXT,process_group_id INTEGER,audio_path TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_safe_jobs_generation ON safe_edit_pipeline_jobs(recording_id,generation,status);
CREATE TABLE IF NOT EXISTS safe_edit_purge_intents(
 purge_id TEXT PRIMARY KEY,recording_id TEXT NOT NULL,generation INTEGER NOT NULL,file_ids_json TEXT NOT NULL,server_status TEXT NOT NULL,
 android_status TEXT NOT NULL,overall_status TEXT NOT NULL,last_error TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS safe_edit_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,recording_id TEXT,event TEXT NOT NULL,details TEXT,created_at TEXT NOT NULL);
''')
            columns={r['name'] for r in c.execute('PRAGMA table_info(safe_edit_staged_candidates)')}
            for name,ddl in {
                'expected_generation':'expected_generation INTEGER','source_file_id':'source_file_id TEXT',
                'source_size':'source_size INTEGER','source_mtime_ns':'source_mtime_ns INTEGER','source_sha256':'source_sha256 TEXT',
                'candidate_sha256':'candidate_sha256 TEXT','expected_duration':'expected_duration REAL',
                'finalized_path':'finalized_path TEXT',
                'finalized_at':'finalized_at TEXT',
            }.items():
                if name not in columns:c.execute(f'ALTER TABLE safe_edit_staged_candidates ADD COLUMN {ddl}')
            c.execute("UPDATE safe_edit_staged_candidates SET finalized_at=created_at WHERE finalized_path IS NOT NULL AND finalized_at IS NULL")
    def _event(self,c,rid,event,details=None): c.execute('INSERT INTO safe_edit_events(recording_id,event,details,created_at) VALUES(?,?,?,?)',(rid,event,json.dumps(details,ensure_ascii=False,sort_keys=True) if isinstance(details,(dict,list)) else details,iso(self.now())))
    def _file(self,c,path:Path)->str:
        path=str(path.resolve());row=c.execute('SELECT file_id FROM safe_edit_files WHERE path=?',(path,)).fetchone()
        if row:return row['file_id']
        fid=uuid.uuid4().hex;c.execute('INSERT INTO safe_edit_files VALUES(?,?,?)',(fid,path,iso(self.now())));return fid
    def register_recording(self,recording_id,canonical_path,state='FINAL',generation=1,parent_recording_id=None):
        self.init_schema();ts=iso(self.now())
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE');fid=self._file(c,Path(canonical_path))
            c.execute('''INSERT OR IGNORE INTO safe_edit_recordings(
                recording_id,current_generation,canonical_file_id,state,parent_recording_id,visible,created_at,updated_at
            ) VALUES(?,?,?,?,?,1,?,?)''',(recording_id,generation,fid,state,parent_recording_id,ts,ts))
            c.execute('INSERT OR IGNORE INTO safe_edit_generations VALUES(?,?,?,?,0,?)',(recording_id,generation,fid,state,ts))
            c.execute('INSERT OR IGNORE INTO safe_edit_file_references VALUES(?,?,?,?,?,?,?)',(uuid.uuid4().hex,fid,recording_id,generation,'CANONICAL_AUDIO',1,ts));c.execute('COMMIT')
    def get_recording(self,rid):
        with self.connect() as c:r=c.execute('SELECT * FROM safe_edit_recordings WHERE recording_id=?',(rid,)).fetchone();return dict(r) if r else None
    def enter_edit_pending(self,rid,grace_seconds=DEFAULT_GRACE_SECONDS):
        ts=self.now();deadline=iso(ts+timedelta(seconds=grace_seconds))
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE');r=c.execute('SELECT * FROM safe_edit_recordings WHERE recording_id=?',(rid,)).fetchone()
            if not r:raise SafeEditError('recording_not_registered')
            if r['state']=='EDITING' and parse_iso(r['edit_lease_expires_at']) and parse_iso(r['edit_lease_expires_at'])>ts:raise SafeEditError('active_edit_lease')
            c.execute('UPDATE safe_edit_recordings SET pre_edit_state=state,state=?,edit_grace_deadline=?,edit_lease_token=NULL,edit_lease_owner=NULL,edit_lease_acquired_at=NULL,edit_lease_expires_at=NULL,edit_lease_heartbeat=NULL,updated_at=? WHERE recording_id=?',('EDIT_PENDING',deadline,iso(ts),rid));self._event(c,rid,'edit_pending',{'deadline':deadline});c.execute('COMMIT')
    def acquire_edit_lease(self,rid,owner,ttl_seconds,token=None):
        token=token or uuid.uuid4().hex;now=self.now();exp=iso(now+timedelta(seconds=ttl_seconds))
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE');r=c.execute('SELECT * FROM safe_edit_recordings WHERE recording_id=?',(rid,)).fetchone()
            if not r or r['state'] not in {'EDIT_PENDING','EDIT_RECOVERY_REQUIRED'}:c.execute('ROLLBACK');raise SafeEditError('edit_not_available')
            c.execute('UPDATE safe_edit_recordings SET state=?,edit_lease_token=?,edit_lease_owner=?,edit_lease_acquired_at=?,edit_lease_expires_at=?,edit_lease_heartbeat=?,updated_at=? WHERE recording_id=?',('EDITING',token,owner,iso(now),exp,iso(now),iso(now),rid));self._event(c,rid,'edit_lease_acquired',{'owner':owner,'expires':exp});c.execute('COMMIT');return token
    def heartbeat_edit_lease(self,rid,token,ttl_seconds):
        now=self.now();exp=iso(now+timedelta(seconds=ttl_seconds))
        with self.connect() as c:
            cur=c.execute('UPDATE safe_edit_recordings SET edit_lease_heartbeat=?,edit_lease_expires_at=?,updated_at=? WHERE recording_id=? AND state=? AND edit_lease_token=? AND edit_lease_expires_at>?',(iso(now),exp,iso(now),rid,'EDITING',token,iso(now)));return cur.rowcount==1
    def auto_confirm_due(self,rid):
        now=self.now()
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE');r=c.execute('SELECT * FROM safe_edit_recordings WHERE recording_id=?',(rid,)).fetchone()
            if not r:c.execute('COMMIT');return False
            if r['state']=='EDITING':
                exp=parse_iso(r['edit_lease_expires_at'])
                if exp and exp<=now:c.execute("UPDATE safe_edit_recordings SET state='EDIT_RECOVERY_REQUIRED',updated_at=? WHERE recording_id=?",(iso(now),rid));self._event(c,rid,'edit_lease_expired_recovery_required')
                c.execute('COMMIT');return False
            if r['state']!='EDIT_PENDING' or not r['edit_grace_deadline'] or parse_iso(r['edit_grace_deadline'])>now:c.execute('COMMIT');return False
            c.execute("UPDATE safe_edit_recordings SET state='CONFIRMED',updated_at=? WHERE recording_id=? AND state='EDIT_PENDING'",(iso(now),rid));ok=c.total_changes>0
            if ok:self._event(c,rid,'auto_confirmed')
            c.execute('COMMIT');return ok
    def recover_expired_leases(self):
        now=self.now();changed=[]
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE');rows=c.execute("SELECT recording_id FROM safe_edit_recordings WHERE state='EDITING' AND edit_lease_expires_at<=?",(iso(now),)).fetchall()
            for r in rows:c.execute("UPDATE safe_edit_recordings SET state='EDIT_RECOVERY_REQUIRED',updated_at=? WHERE recording_id=?",(iso(now),r['recording_id']));self._event(c,r['recording_id'],'edit_lease_expired_recovery_required');changed.append(r['recording_id'])
            c.execute('COMMIT')
        return changed
    def cancel_edit(self,rid,token):
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE');r=c.execute('SELECT * FROM safe_edit_recordings WHERE recording_id=? AND edit_lease_token=?',(rid,token)).fetchone()
            if not r:raise SafeEditError('lease_mismatch')
            restore=r['pre_edit_state'] or 'FINAL';c.execute('UPDATE safe_edit_recordings SET state=?,pre_edit_state=NULL,edit_grace_deadline=NULL,edit_lease_token=NULL,edit_lease_owner=NULL,edit_lease_acquired_at=NULL,edit_lease_expires_at=NULL,edit_lease_heartbeat=NULL,updated_at=? WHERE recording_id=?',(restore,iso(self.now()),rid));c.execute('DELETE FROM safe_edit_staged_candidates WHERE recording_id=?',(rid,));self._event(c,rid,'edit_cancelled',{'restored':restore});c.execute('COMMIT')
    def storage_preflight(self,*,estimated_output_total,temp_overhead,safety_margin,free_bytes=None,path=None):
        required=int(estimated_output_total)+int(temp_overhead)+int(safety_margin)
        free=int(free_bytes if free_bytes is not None else shutil.disk_usage(str(path or self.db_path.parent)).free)
        return {'ok':free>=required,'required_bytes':required,'free_bytes':free,'error':None if free>=required else 'EDIT_STORAGE_INSUFFICIENT'}
    def estimate_storage(self,*,operation,source_size,total_duration_sec,output_durations_sec):
        if operation not in {'TRIM','SPLIT'} or source_size<=0 or total_duration_sec<=0:raise SafeEditError('storage_estimate_inputs_invalid')
        ratio=sum(float(x) for x in output_durations_sec)/float(total_duration_sec)
        estimated_output=max(1,int(source_size*ratio*1.10))
        # Phase88C contract is direct AAC/M4A encode into same-filesystem staging;
        # it does not materialize a full PCM copy.
        temp_overhead=max(8*1024*1024,int(estimated_output*0.05))
        safety_margin=max(64*1024*1024,int(estimated_output*0.10))
        return {'strategy':'FFMPEG_DECODE_ENCODE_AAC_M4A','estimated_output_total':estimated_output,'estimated_temp_overhead':temp_overhead,'safety_margin':safety_margin,'required_bytes':estimated_output+temp_overhead+safety_margin}
    def validate_staging_boundary(self,staging_dir,final_dir):
        a=Path(staging_dir);b=Path(final_dir);a.mkdir(parents=True,exist_ok=True);b.mkdir(parents=True,exist_ok=True)
        same=a.stat().st_dev==b.stat().st_dev
        return {'ok':same,'same_filesystem':same,'error':None if same else 'STAGING_NOT_ATOMIC_RENAME_BOUNDARY'}
    def _identity(self,path):
        p=Path(path);h=hashlib.sha256()
        with p.open('rb') as f:
            for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
        st=p.stat();return {'size':st.st_size,'mtime_ns':st.st_mtime_ns,'sha256':h.hexdigest()}
    def register_staged_candidate(self,*,op_id,recording_id,role,path,validation,expected_generation=None):
        p=Path(path).resolve()
        with self.connect() as c:
            rec=c.execute('SELECT * FROM safe_edit_recordings WHERE recording_id=?',(recording_id,)).fetchone()
            if not rec:raise SafeEditError('recording_not_registered')
            generation=int(expected_generation if expected_generation is not None else rec['current_generation'])
            source=c.execute('SELECT path FROM safe_edit_files WHERE file_id=?',(rec['canonical_file_id'],)).fetchone()
            if not source or not Path(source['path']).is_file():raise SafeEditError('canonical_source_missing')
            source_id=self._identity(source['path']);candidate_id=self._identity(p) if p.is_file() else {'sha256':None}
            c.execute('''INSERT OR REPLACE INTO safe_edit_staged_candidates(
                op_id,recording_id,role,path,validation_json,validated,created_at,expected_generation,source_file_id,
                source_size,source_mtime_ns,source_sha256,candidate_sha256,expected_duration
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(op_id,recording_id,role,str(p),json.dumps(validation,sort_keys=True),1 if validation.get('ok') else 0,iso(self.now()),generation,rec['canonical_file_id'],source_id['size'],source_id['mtime_ns'],source_id['sha256'],candidate_id.get('sha256'),validation.get('duration_sec')))
    def seed_pipeline_job(self,jid,rid,generation,audio_path,status='queued'):
        ts=iso(self.now())
        with self.connect() as c:c.execute('INSERT INTO safe_edit_pipeline_jobs VALUES(?,?,?,?,0,NULL,NULL,NULL,?,?,?)',(jid,rid,generation,status,str(Path(audio_path).resolve()),ts,ts))
    def set_job_running(self,jid,worker,token,pgid=None):
        with self.connect() as c:c.execute("UPDATE safe_edit_pipeline_jobs SET status='running',claimed_by=?,claim_token=?,process_group_id=?,updated_at=? WHERE job_id=?",(worker,token,pgid,iso(self.now()),jid))
    def get_job(self,jid):
        with self.connect() as c:r=c.execute('SELECT * FROM safe_edit_pipeline_jobs WHERE job_id=?',(jid,)).fetchone();return dict(r) if r else None
    def assert_current_generation(self,rid,generation,checkpoint='work'):
        r=self.get_recording(rid)
        if not r or int(r['current_generation'])!=int(generation):raise StaleGeneration(f'{checkpoint}: {rid} generation={generation} current={r["current_generation"] if r else None}')
        return True
    def can_reconcile_job(self,jid):
        j=self.get_job(jid)
        if not j or j['cancel_requested'] or j['status']=='cancelled':return False
        try:self.assert_current_generation(j['recording_id'],j['generation'],'reconcile')
        except StaleGeneration:return False
        return True
    def _require_switch(self,enable_for_test):
        if not (self.destructive_enabled or enable_for_test):raise SafeEditError('destructive_edit_disabled')
    def _verified_lease(self,c,rid,expected,token):
        r=c.execute('SELECT * FROM safe_edit_recordings WHERE recording_id=?',(rid,)).fetchone();now=self.now()
        if not r or int(r['current_generation'])!=int(expected):raise StaleGeneration('expected_current_generation_mismatch')
        if r['state']!='EDITING' or r['edit_lease_token']!=token or not r['edit_lease_expires_at'] or parse_iso(r['edit_lease_expires_at'])<=now:raise SafeEditError('active_lease_required')
        return r
    def _candidate(self,c,op,rid,role):
        r=c.execute('SELECT * FROM safe_edit_staged_candidates WHERE op_id=? AND recording_id=? AND role=?',(op,rid,role)).fetchone()
        if not r or not r['validated']:raise SafeEditError(f'{role}_not_validated')
        return r
    def _revalidate_and_finalize_candidate(self,c,cand,new_generation,final_root=None):
        rec=c.execute('SELECT * FROM safe_edit_recordings WHERE recording_id=?',(cand['recording_id'],)).fetchone()
        if not rec or int(rec['current_generation'])!=int(cand['expected_generation']):raise StaleGeneration('candidate_expected_generation_changed')
        if rec['canonical_file_id']!=cand['source_file_id']:raise StaleGeneration('candidate_source_changed')
        source=c.execute('SELECT path FROM safe_edit_files WHERE file_id=?',(cand['source_file_id'],)).fetchone()
        if not source or not Path(source['path']).is_file():raise SafeEditError('canonical_source_missing')
        sid=self._identity(source['path'])
        if sid!={'size':cand['source_size'],'mtime_ns':cand['source_mtime_ns'],'sha256':cand['source_sha256']}:raise SafeEditError('canonical_source_identity_changed')
        staged=Path(cand['path'])
        validation=validate_audio_file(staged,expected_duration=cand['expected_duration'])
        if not validation['ok']:raise SafeEditError('candidate_revalidation_failed:'+','.join(validation['errors']))
        if self._identity(staged)['sha256']!=cand['candidate_sha256']:raise SafeEditError('candidate_identity_changed')
        root=Path(final_root) if final_root else Path(source['path']).parent
        final_dir=root/'generations'/f'g{int(new_generation):06d}'
        final_dir.mkdir(parents=True,exist_ok=True)
        if staged.stat().st_dev!=final_dir.stat().st_dev:raise SafeEditError('STAGING_NOT_ATOMIC_RENAME_BOUNDARY')
        final_path=final_dir/f'audio{staged.suffix.lower()}'
        if final_path.exists():raise SafeEditError('immutable_final_already_exists')
        os.replace(staged,final_path)
        fd=os.open(final_dir,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
        final_validation=validate_audio_file(final_path,expected_duration=cand['expected_duration'])
        if not final_validation['ok']:raise SafeEditError('final_candidate_validation_failed')
        c.execute('UPDATE safe_edit_staged_candidates SET finalized_path=?,finalized_at=? WHERE op_id=? AND role=?',(str(final_path),iso(self.now()),cand['op_id'],cand['role']))
        return final_path

    def cleanup_orphan_finalized_candidates(self,min_age_seconds=300):
        """Remove only finalized candidates not reachable from any live reference."""
        removed=[];failed=[]
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            rows=c.execute("SELECT op_id,role,finalized_path,finalized_at FROM safe_edit_staged_candidates WHERE finalized_path IS NOT NULL").fetchall()
            for row in rows:
                finalized_at=parse_iso(row['finalized_at']) if row['finalized_at'] else None
                if not finalized_at or (self.now()-finalized_at).total_seconds()<int(min_age_seconds):continue
                path=Path(row['finalized_path'])
                live=int(c.execute('''SELECT COUNT(*) c FROM safe_edit_file_references r JOIN safe_edit_files f ON f.file_id=r.file_id WHERE f.path=? AND r.active=1''',(str(path.resolve()),)).fetchone()['c'])
                if live:continue
                try:
                    if path.exists():path.unlink()
                    c.execute('UPDATE safe_edit_staged_candidates SET finalized_path=NULL WHERE op_id=? AND role=?',(row['op_id'],row['role']))
                    removed.append(str(path))
                except Exception as exc:failed.append({'path':str(path),'error':repr(exc)})
            c.execute('COMMIT')
        return {'removed':removed,'failed':failed}
    def _purge_intent(self,c,rid,generation,carry_to_generation=None):
        refs=c.execute('SELECT file_id,role FROM safe_edit_file_references WHERE recording_id=? AND generation=? AND active=1',(rid,generation)).fetchall();fids=[x['file_id'] for x in refs]
        c.execute('UPDATE safe_edit_file_references SET active=0 WHERE recording_id=? AND generation=?',(rid,generation))
        if carry_to_generation is not None:
            # The production trim job deliberately reuses its output directory.
            # Carry non-audio paths forward before the old-generation purge can
            # race the new worker; stage-generation markers prevent stale reuse.
            ts=iso(self.now())
            for ref in refs:
                if ref['role']=='CANONICAL_AUDIO':
                    continue
                c.execute('INSERT OR IGNORE INTO safe_edit_file_references VALUES(?,?,?,?,?,?,?)',
                          (uuid.uuid4().hex,ref['file_id'],rid,int(carry_to_generation),ref['role'],1,ts))
        pid=uuid.uuid4().hex;ts=iso(self.now())
        c.execute('INSERT INTO safe_edit_purge_intents VALUES(?,?,?,?,?,?,?,?,?,?)',(pid,rid,generation,json.dumps(fids),'PURGE_PENDING','PURGE_PENDING_ANDROID','PURGE_PENDING',None,ts,ts));return pid
    def _has_production_jobs(self,c):
        return c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recording_jobs'").fetchone() is not None
    def _replace_production_trim_job(self,c,rid,newg,audio_path,ts):
        if not self._has_production_jobs(c):return None
        row=c.execute('SELECT * FROM recording_jobs WHERE recording_id=?',(rid,)).fetchone()
        if not row:return None
        jid=f'vocanote_{rid}_g{newg}_{uuid.uuid4().hex[:8]}'
        c.execute('''UPDATE recording_jobs SET job_id=?,generation=?,status='queued',step='queued',audio_path=?,
            attempts=0,claimed_at=NULL,claimed_by=NULL,claim_token=NULL,heartbeat_at=NULL,lease_expires_at=NULL,
            cancel_requested=0,process_group_id=NULL,error_code=NULL,error_message=NULL,result_json_path=NULL,updated_at=?
            WHERE recording_id=?''',(jid,newg,audio_path,ts,rid))
        return jid
    def _insert_production_split_jobs(self,c,parent,children,ts):
        if not self._has_production_jobs(c):return
        old=c.execute('SELECT * FROM recording_jobs WHERE recording_id=?',(parent,)).fetchone()
        if old:
            c.execute("UPDATE recording_jobs SET status='cancelled',step='generation_superseded',cancel_requested=1,claimed_by=NULL,claim_token=NULL,heartbeat_at=NULL,lease_expires_at=NULL,updated_at=? WHERE recording_id=?",(ts,parent))
        for rid,audio_path in children:
            output_dir=str(Path(audio_path).parent);metadata_path=str(Path(output_dir)/'metadata.json')
            c.execute('''INSERT INTO recording_jobs(job_id,recording_id,status,step,audio_path,metadata_path,output_dir,
                attempts,max_attempts,created_at,updated_at,generation,cancel_requested)
                VALUES(?,?, 'queued','queued',?,?,?,0,3,?,?,1,0)''',
                (f'vocanote_{rid}_g1_{uuid.uuid4().hex[:8]}',rid,audio_path,metadata_path,output_dir,ts,ts))
    def confirm_trim(self,op_id,rid,expected_generation,lease_token,*,enable_for_test=False,failpoint=None):
        self._require_switch(enable_for_test)
        # Filesystem finalization precedes the authoritative DB switch. A crash
        # here can leave only an invisible orphan, never a new current generation.
        with self.connect() as pre:
            self._verified_lease(pre,rid,expected_generation,lease_token)
            cand=self._candidate(pre,op_id,rid,'TRIM')
            final_path=self._revalidate_and_finalize_candidate(pre,cand,int(expected_generation)+1)
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE');committed=False
            try:
                self._verified_lease(c,rid,expected_generation,lease_token)
                newg=int(expected_generation)+1;ts=iso(self.now());fid=self._file(c,final_path)
                c.execute("UPDATE safe_edit_generations SET superseded=1,state='SUPERSEDED' WHERE recording_id=? AND generation=?",(rid,expected_generation))
                pid=self._purge_intent(c,rid,expected_generation,carry_to_generation=newg)
                c.execute('INSERT INTO safe_edit_generations VALUES(?,?,?,?,0,?)',(rid,newg,fid,'PROCESSING',ts));c.execute('INSERT INTO safe_edit_file_references VALUES(?,?,?,?,?,?,?)',(uuid.uuid4().hex,fid,rid,newg,'CANONICAL_AUDIO',1,ts))
                c.execute("UPDATE safe_edit_pipeline_jobs SET cancel_requested=1,status='cancelled',claim_token=NULL,updated_at=? WHERE recording_id=? AND generation=?",(ts,rid,expected_generation))
                c.execute('INSERT INTO safe_edit_pipeline_jobs VALUES(?,?,?,?,0,NULL,NULL,NULL,?,?,?)',(f'phase88_{rid}_g{newg}',rid,newg,'queued',str(final_path),ts,ts))
                self._replace_production_trim_job(c,rid,newg,str(final_path),ts)
                c.execute("UPDATE safe_edit_recordings SET current_generation=?,canonical_file_id=?,state='PROCESSING',pre_edit_state=NULL,edit_grace_deadline=NULL,edit_lease_token=NULL,edit_lease_owner=NULL,edit_lease_acquired_at=NULL,edit_lease_expires_at=NULL,edit_lease_heartbeat=NULL,updated_at=? WHERE recording_id=?",(newg,fid,ts,rid));self._event(c,rid,'canonical_switched',{'old_generation':expected_generation,'new_generation':newg,'purge_id':pid})
                if failpoint=='before_commit':raise RuntimeError('phase88_failpoint_before_commit')
                c.execute('COMMIT');committed=True
                result={'recording_id':rid,'new_generation':newg,'canonical_path':str(final_path),'purge_id':pid,'purge_status':'PURGE_PENDING'}
                if failpoint=='after_commit':raise RuntimeError('phase88_failpoint_after_commit')
                return result
            except Exception:
                if not committed:c.execute('ROLLBACK')
                raise
    def confirm_split(self,op_id,parent,expected_generation,lease_token,child_a,child_b,*,enable_for_test=False,failpoint=None):
        self._require_switch(enable_for_test)
        finalized=[]
        try:
            with self.connect() as pre:
                rec=self._verified_lease(pre,parent,expected_generation,lease_token)
                a=self._candidate(pre,op_id,parent,'SPLIT_A');b=self._candidate(pre,op_id,parent,'SPLIT_B')
                source=pre.execute('SELECT path FROM safe_edit_files WHERE file_id=?',(rec['canonical_file_id'],)).fetchone()
                source_parent=Path(source['path']).parent
                library_root=source_parent.parent if source_parent.name==parent else source_parent/'split_children'
                pa=self._revalidate_and_finalize_candidate(pre,a,1,library_root/child_a);finalized.append(pa)
                pb=self._revalidate_and_finalize_candidate(pre,b,1,library_root/child_b);finalized.append(pb)
        except Exception:
            for path in finalized:
                try:path.unlink(missing_ok=True)
                except OSError:pass
            raise
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            try:
                self._verified_lease(c,parent,expected_generation,lease_token);ts=iso(self.now())
                for rid,path in ((child_a,pa),(child_b,pb)):
                    fid=self._file(c,path);c.execute('''INSERT INTO safe_edit_recordings(
                        recording_id,current_generation,canonical_file_id,state,parent_recording_id,visible,created_at,updated_at
                    ) VALUES(?,?,?,?,?,1,?,?)''',(rid,1,fid,'PROCESSING',parent,ts,ts));c.execute('INSERT INTO safe_edit_generations VALUES(?,?,?,?,0,?)',(rid,1,fid,'PROCESSING',ts));c.execute('INSERT INTO safe_edit_file_references VALUES(?,?,?,?,?,?,?)',(uuid.uuid4().hex,fid,rid,1,'CANONICAL_AUDIO',1,ts));c.execute('INSERT INTO safe_edit_pipeline_jobs VALUES(?,?,?,?,0,NULL,NULL,NULL,?,?,?)',(f'phase88_{rid}_g1',rid,1,'queued',str(path),ts,ts))
                pid=self._purge_intent(c,parent,expected_generation);c.execute("UPDATE safe_edit_generations SET superseded=1,state='SUPERSEDED' WHERE recording_id=? AND generation=?",(parent,expected_generation));c.execute("UPDATE safe_edit_pipeline_jobs SET cancel_requested=1,status='cancelled',claim_token=NULL,updated_at=? WHERE recording_id=? AND generation=?",(ts,parent,expected_generation));self._insert_production_split_jobs(c,parent,[(child_a,str(pa)),(child_b,str(pb))],ts);c.execute("UPDATE safe_edit_recordings SET visible=0,state='RETIRED',edit_lease_token=NULL,updated_at=? WHERE recording_id=?",(ts,parent));self._event(c,parent,'split_committed',{'children':[child_a,child_b],'purge_id':pid})
                if failpoint=='before_commit':raise RuntimeError('phase88_failpoint_before_commit')
                c.execute('COMMIT');return {'children':[child_a,child_b],'canonical_paths':[str(pa),str(pb)],'visible_final_count':2,'purge_id':pid}
            except Exception:c.execute('ROLLBACK');raise
    def count_visible_children(self,parent):
        with self.connect() as c:return int(c.execute('SELECT COUNT(*) c FROM safe_edit_recordings WHERE parent_recording_id=? AND visible=1',(parent,)).fetchone()['c'])
    def add_file_reference_for_test(self,rid,generation,path,role):
        with self.connect() as c:fid=self._file(c,Path(path));c.execute('INSERT INTO safe_edit_file_references VALUES(?,?,?,?,?,?,?)',(uuid.uuid4().hex,fid,rid,generation,role,1,iso(self.now())))
    def register_generation_artifacts(self,rid,generation,artifacts):
        """Register concrete files; row references, not a counter, are authoritative."""
        ts=iso(self.now())
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            try:
                current=c.execute('SELECT current_generation FROM safe_edit_recordings WHERE recording_id=?',(rid,)).fetchone()
                if not current or int(current['current_generation'])!=int(generation):raise StaleGeneration('artifact_registration_stale_generation')
                for role,path in artifacts:
                    if role not in REFERENCE_ROLES:raise SafeEditError(f'unknown_reference_role:{role}')
                    p=Path(path)
                    if p.is_dir():raise SafeEditError('artifact_reference_must_be_file')
                    fid=self._file(c,p)
                    # A file that has ever been canonical audio cannot be
                    # reclassified as a later generation's generic artifact
                    # while its committed purge is pending.
                    canonical=c.execute("SELECT 1 FROM safe_edit_file_references WHERE file_id=? AND role='CANONICAL_AUDIO' LIMIT 1",(fid,)).fetchone()
                    if canonical and role!='CANONICAL_AUDIO':continue
                    c.execute('INSERT OR IGNORE INTO safe_edit_file_references VALUES(?,?,?,?,?,?,?)',(uuid.uuid4().hex,fid,rid,generation,role,1,ts))
                c.execute('COMMIT')
            except Exception:c.execute('ROLLBACK');raise
    def register_artifacts_if_cataloged(self,rid,generation,artifacts):
        """Attach persistent production outputs to the authoritative generation graph."""
        with self.connect() as c:
            current=c.execute('SELECT current_generation FROM safe_edit_recordings WHERE recording_id=?',(rid,)).fetchone()
        if not current:return {'cataloged':False,'registered':0}
        if int(current['current_generation'])!=int(generation):raise StaleGeneration('artifact_registration_stale_generation')
        material=[]
        for role,path in artifacts:
            p=Path(path)
            if p.is_file():material.append((role,p))
        self.register_generation_artifacts(rid,generation,material)
        return {'cataloged':True,'registered':len(material)}
    def live_reference_count_for_path(self,path):
        with self.connect() as c:r=c.execute('SELECT COUNT(*) c FROM safe_edit_file_references r JOIN safe_edit_files f ON f.file_id=r.file_id WHERE f.path=? AND r.active=1',(str(Path(path).resolve()),)).fetchone();return int(r['c'])
    def list_purge_intents(self):
        with self.connect() as c:return [dict(x) for x in c.execute('SELECT * FROM safe_edit_purge_intents ORDER BY created_at')]
    def list_android_pending_purges(self):
        with self.connect() as c:
            rows=c.execute("SELECT purge_id,recording_id,generation,created_at FROM safe_edit_purge_intents WHERE android_status!='ANDROID_PURGED' ORDER BY created_at").fetchall()
            return [dict(x) for x in rows]
    def get_purge(self,pid):
        with self.connect() as c:r=c.execute('SELECT * FROM safe_edit_purge_intents WHERE purge_id=?',(pid,)).fetchone();return dict(r) if r else None
    def _refresh_overall(self,c,pid):
        r=c.execute('SELECT server_status,android_status FROM safe_edit_purge_intents WHERE purge_id=?',(pid,)).fetchone();overall='PURGE_COMPLETE' if r['server_status']=='SERVER_PURGED' and r['android_status']=='ANDROID_PURGED' else 'PURGE_PENDING';c.execute('UPDATE safe_edit_purge_intents SET overall_status=?,updated_at=? WHERE purge_id=?',(overall,iso(self.now()),pid))
    def ack_android_purge(self,pid):
        with self.connect() as c:c.execute("UPDATE safe_edit_purge_intents SET android_status='ANDROID_PURGED',updated_at=? WHERE purge_id=?",(iso(self.now()),pid));self._refresh_overall(c,pid)
    def purge_server(self,*,outstanding_only=True,unlink_fn=None):
        unlink_fn=unlink_fn or (lambda p:Path(p).unlink(missing_ok=True));done=failed=0
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            query="SELECT * FROM safe_edit_purge_intents WHERE server_status!='SERVER_PURGED'" if outstanding_only else 'SELECT * FROM safe_edit_purge_intents'
            for p in c.execute(query).fetchall():
                try:
                    for fid in json.loads(p['file_ids_json']):
                        live=int(c.execute('SELECT COUNT(*) c FROM safe_edit_file_references WHERE file_id=? AND active=1',(fid,)).fetchone()['c'])
                        row=c.execute('SELECT path FROM safe_edit_files WHERE file_id=?',(fid,)).fetchone()
                        if live==0 and row:
                            target=Path(row['path'])
                            if target.exists():
                                unlink_fn(str(target))
                            if target.exists():
                                raise SafeEditError(f'purge_delete_not_verified:{target}')
                    c.execute("UPDATE safe_edit_purge_intents SET server_status='SERVER_PURGED',last_error=NULL,updated_at=? WHERE purge_id=?",(iso(self.now()),p['purge_id']));self._refresh_overall(c,p['purge_id']);done+=1
                except Exception as e:c.execute("UPDATE safe_edit_purge_intents SET server_status='PURGE_PENDING',last_error=?,updated_at=? WHERE purge_id=?",(f'{type(e).__name__}: {e}',iso(self.now()),p['purge_id']));failed+=1
            c.execute('COMMIT')
        return {'purged':done,'failed':failed}
    def cancellation_plan(self,jid,grace_seconds=10):
        j=self.get_job(jid)
        return {'job_id':jid,'process_group_id':j.get('process_group_id') if j else None,'steps':['cancel_requested','cooperative_wait','SIGTERM_PROCESS_GROUP','SIGKILL_PROCESS_GROUP'],'grace_seconds':grace_seconds}
    def terminate_registered_process(self,jid,*,killpg=os.killpg,grace_expired=True):
        j=self.get_job(jid);pgid=j.get('process_group_id') if j else None
        if not pgid:return {'signalled':False,'reason':'no_process_group'}
        killpg(int(pgid),signal.SIGTERM)
        if grace_expired:killpg(int(pgid),signal.SIGKILL)
        return {'signalled':True,'process_group_id':int(pgid),'sigkill':bool(grace_expired)}
