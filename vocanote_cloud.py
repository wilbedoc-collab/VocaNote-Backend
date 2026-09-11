#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import mimetypes
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

SERVER_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()
TOKEN_FILE=Path(os.environ.get('VOCANOTE_GOOGLE_TOKEN', str(SERVER_DIR / 'vocanote_google_token.json'))).expanduser()
ROOT_FOLDER = os.environ.get('VOCANOTE_DRIVE_ROOT', 'VocaNote')
AUDIO_ROOT = os.environ.get('VOCANOTE_DRIVE_AUDIO_ROOT', 'Audio')
CACHE_DIR = Path(os.environ.get('VOCANOTE_CLOUD_CACHE_DIR', str(SERVER_DIR / 'cloud_audio_cache'))).resolve()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f'.{path.name}.{os.getpid()}.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def write_json(path: Path, data: Any) -> None:
    atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2))


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def file_hashes(path: Path) -> tuple[str, str]:
    md5 = hashlib.md5()
    sha256 = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            md5.update(chunk)
            sha256.update(chunk)
    return md5.hexdigest(), sha256.hexdigest()


def drive_service():
    data = json.loads(TOKEN_FILE.read_text(encoding='utf-8'))
    creds = Credentials.from_authorized_user_info(data, scopes=data.get('scopes'))
    if not creds.valid and creds.refresh_token:
        creds.refresh(Request())
        write_json(TOKEN_FILE, json.loads(creds.to_json()))
    return build('drive', 'v3', credentials=creds, cache_discovery=False)


def get_or_create_folder(service, name: str, parent_id: str | None = None) -> str:
    safe_name = name.replace("'", "\\'")
    q = f"name='{safe_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    res = service.files().list(q=q, spaces='drive', fields='files(id,name)', pageSize=10).execute()
    files = res.get('files', [])
    if files:
        return files[0]['id']
    meta: dict[str, Any] = {'name': name, 'mimeType': 'application/vnd.google-apps.folder'}
    if parent_id:
        meta['parents'] = [parent_id]
    folder = service.files().create(body=meta, fields='id').execute()
    return folder['id']


def ensure_audio_folder(service, recorded_at: str) -> tuple[str, str]:
    year, month = 'unknown', 'unknown'
    if recorded_at and len(recorded_at) >= 7:
        year = recorded_at[:4]
        month = recorded_at[5:7]
    root = get_or_create_folder(service, ROOT_FOLDER)
    audio = get_or_create_folder(service, AUDIO_ROOT, root)
    y = get_or_create_folder(service, year, audio)
    m = get_or_create_folder(service, month, y)
    return m, f'{ROOT_FOLDER}/{AUDIO_ROOT}/{year}/{month}'


def update_metadata_state(recording_dir: Path, *, write_json_fn=None, **updates: Any) -> dict[str, Any]:
    meta_path = recording_dir / 'metadata.json'
    meta = load_json(meta_path)
    meta.update(updates)
    meta['updated_at'] = now_iso()
    (write_json_fn or write_json)(meta_path, meta)
    return meta


def upload_recording_audio_to_drive(recording_dir: Path, *, write_json_fn=None, assert_claim=None, preserve_local: bool = False) -> dict[str, Any]:
    if assert_claim:
        assert_claim()
    meta = load_json(recording_dir / 'metadata.json')
    rid = meta['recording_id']
    audio_name = meta.get('audio_file') or 'audio.m4a'
    audio_path = recording_dir / audio_name
    if meta.get('audio_storage_state') == 'CLOUD' and meta.get('cloud_file_id'):
        return meta
    if not audio_path.exists():
        if meta.get('cloud_file_id'):
            return meta
        raise FileNotFoundError(f'local audio missing and no cloud_file_id: {audio_path}')

    size = audio_path.stat().st_size
    md5, sha256 = file_hashes(audio_path)
    meta = update_metadata_state(
        recording_dir,
        write_json_fn=write_json_fn,
        audio_storage_state='CLOUD_UPLOAD_PENDING',
        cloud_provider='google_drive',
        local_audio_path=str(audio_path),
        audio_size=size,
        audio_hash=sha256,
        audio_md5=md5,
    )
    service = drive_service()
    folder_id, cloud_dir = ensure_audio_folder(service, meta.get('recorded_at') or meta.get('uploaded_at') or '')
    ext = audio_path.suffix.lower() or '.m4a'
    cloud_name = f'{rid}{ext}'
    mime = mimetypes.guess_type(audio_path.name)[0] or 'audio/mp4'
    meta = update_metadata_state(recording_dir, write_json_fn=write_json_fn, audio_storage_state='CLOUD_UPLOADING')

    # Idempotent remote lookup by immutable filename in target folder.
    q = f"name='{cloud_name}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(q=q, spaces='drive', fields='files(id,name,size,md5Checksum)', pageSize=10).execute().get('files', [])
    if existing:
        file_id = existing[0]['id']
    else:
        media = MediaFileUpload(str(audio_path), mimetype=mime, resumable=True)
        body = {'name': cloud_name, 'parents': [folder_id]}
        created = service.files().create(body=body, media_body=media, fields='id,name,size,md5Checksum').execute()
        file_id = created['id']

    remote = service.files().get(fileId=file_id, fields='id,name,size,md5Checksum,webViewLink').execute()
    remote_size = int(remote.get('size') or -1)
    remote_md5 = remote.get('md5Checksum')
    if remote_size != size:
        update_metadata_state(recording_dir, write_json_fn=write_json_fn, audio_storage_state='CLOUD_UPLOAD_FAILED', cloud_upload_error=f'size mismatch local={size} remote={remote_size}')
        raise RuntimeError(f'cloud size mismatch local={size} remote={remote_size}')
    if remote_md5 and remote_md5 != md5:
        update_metadata_state(recording_dir, write_json_fn=write_json_fn, audio_storage_state='CLOUD_UPLOAD_FAILED', cloud_upload_error='md5 mismatch')
        raise RuntimeError('cloud md5 mismatch')

    meta = update_metadata_state(
        recording_dir,
        write_json_fn=write_json_fn,
        audio_storage_state='LOCAL_AND_CLOUD',
        cloud_provider='google_drive',
        cloud_file_id=file_id,
        cloud_path=f'{cloud_dir}/{cloud_name}',
        cloud_uploaded_at=now_iso(),
        cloud_verified_at=now_iso(),
        cloud_size=remote_size,
        cloud_md5=remote_md5,
    )
    if assert_claim:
        assert_claim()
    # Phase88 safe-edit foundation forbids direct archive cleanup. Canonical
    # deletion is performed only by the row-referenced purge protocol.
    return meta


def download_cloud_audio(recording_dir: Path, ttl_seconds: int = 3600) -> Path:
    meta = load_json(recording_dir / 'metadata.json')
    audio_name = meta.get('audio_file') or 'audio.m4a'
    local = recording_dir / audio_name
    if local.exists():
        return local
    file_id = meta.get('cloud_file_id')
    if not file_id:
        raise FileNotFoundError('no local audio and no cloud_file_id')
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache = CACHE_DIR / f"{meta['recording_id']}_{audio_name}"
    if cache.exists() and cache.stat().st_size == int(meta.get('audio_size') or meta.get('cloud_size') or cache.stat().st_size):
        return cache
    service = drive_service()
    req = service.files().get_media(fileId=file_id)
    with cache.open('wb') as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    expected = int(meta.get('audio_size') or meta.get('cloud_size') or 0)
    if expected and cache.stat().st_size != expected:
        cache.unlink(missing_ok=True)
        raise RuntimeError('cloud cache size mismatch')
    return cache


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('recording_dir')
    args = ap.parse_args()
    result = upload_recording_audio_to_drive(Path(args.recording_dir))
    print(json.dumps({k: result.get(k) for k in ['recording_id','audio_storage_state','cloud_file_id','cloud_path','audio_size']}, ensure_ascii=False, indent=2))
