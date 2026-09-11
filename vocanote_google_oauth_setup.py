#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import json
from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import io, os, tempfile, hashlib

BASE=Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server')
CRED=BASE/'vocanote_google_credentials.json'
TOKEN=BASE/'vocanote_google_token.json'
SCOPES=['https://www.googleapis.com/auth/drive.file']


def save_token(creds):
    TOKEN.write_text(creds.to_json(), encoding='utf-8')
    os.chmod(TOKEN, 0o600)


def get_creds():
    if not CRED.exists():
        raise SystemExit(f'missing credentials: {CRED}')
    creds=None
    if TOKEN.exists():
        creds=Credentials.from_authorized_user_file(str(TOKEN), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow=InstalledAppFlow.from_client_secrets_file(str(CRED), SCOPES)
            creds=flow.run_local_server(port=0)
        save_token(creds)
    return creds


def main():
    creds=get_creds()
    svc=build('drive','v3',credentials=creds,cache_discovery=False)
    tmp=BASE/'vocanote_drive_probe.txt'
    content=b'VocaNote Drive API probe\n'
    tmp.write_bytes(content)
    media=MediaFileUpload(str(tmp),mimetype='text/plain',resumable=False)
    meta={'name':'vocanote_drive_probe.txt'}
    f=svc.files().create(body=meta,media_body=media,fields='id,name,size,md5Checksum').execute()
    req=svc.files().get_media(fileId=f['id'])
    buf=io.BytesIO()
    dl=MediaIoBaseDownload(buf,req)
    done=False
    while not done:
        _,done=dl.next_chunk()
    ok=buf.getvalue()==content and int(f.get('size') or 0)==len(content)
    print(json.dumps({'ok':ok,'file_id':f['id'],'name':f['name'],'size':f.get('size'),'scope':'drive.file'},ensure_ascii=False,indent=2))

if __name__=='__main__':
    main()
