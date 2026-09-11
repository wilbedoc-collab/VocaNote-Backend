#!/usr/bin/env python3
from pathlib import Path
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from google_auth_oauthlib.flow import InstalledAppFlow

BASE=Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server')
SCOPES=['https://www.googleapis.com/auth/drive.file']
CRED=BASE/'vocanote_google_credentials.json'
TOKEN=BASE/'vocanote_google_token.json'
PORT=0

class Handler(BaseHTTPRequestHandler):
    code_value=None
    error_value=None
    def log_message(self, fmt, *args):
        return
    def do_GET(self):
        qs=parse_qs(urlparse(self.path).query)
        Handler.code_value=(qs.get('code') or [None])[0]
        Handler.error_value=(qs.get('error') or [None])[0]
        self.send_response(200)
        self.send_header('Content-Type','text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write('VocaNote Google Drive authorization received. You can close this window.'.encode('utf-8'))

httpd=HTTPServer(('127.0.0.1', PORT), Handler)
port=httpd.server_address[1]
flow=InstalledAppFlow.from_client_secrets_file(str(CRED), SCOPES)
flow.redirect_uri=f'http://127.0.0.1:{port}/'
auth_url,_=flow.authorization_url(access_type='offline', include_granted_scopes='false', prompt='consent')
(BASE/'vocanote_auth_url.txt').write_text(auth_url, encoding='utf-8')
print('AUTH_URL_WRITTEN', flush=True)
httpd.handle_request()
if Handler.error_value:
    raise SystemExit('oauth_error='+Handler.error_value)
if not Handler.code_value:
    raise SystemExit('no_code_received')
flow.fetch_token(code=Handler.code_value)
TOKEN.write_text(flow.credentials.to_json(), encoding='utf-8')
os.chmod(TOKEN,0o600)
print('TOKEN_SAVED', flush=True)
