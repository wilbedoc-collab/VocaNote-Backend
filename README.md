# VocaNote Backend

Backend service and worker pipeline for VocaNote recordings.

## What this backend does

- FastAPI upload/list/detail/download/delete API for recordings.
- SQLite-backed processing queue.
- Production STT pipeline using `whisper.cpp` + Metal + `large-v3` + `-mc 0`.
- OpenAI Whisper `large-v3` rollback path via environment variable.
- Chunked Hermes transcript correction.
- Semantic map/reduce note intelligence.
- Deterministic rendering to summary/analysis markdown.
- Tombstone/fencing safeguards for delete and stale-worker recovery.
- Optional Google Drive cloud audio archive hooks.

## Security note

This public repository intentionally excludes runtime data and secrets:

- upload token
- SQLite queue database
- recording audio/transcripts/results
- logs
- Google OAuth credentials/tokens
- validation run artifacts

Use `.env.example` as a starting point for local configuration.

## Runtime layout

The production deployment used during development stores recording artifacts outside this repo, typically under a separate `recordings/` directory. Do not commit real patient/meeting audio, transcripts, or generated notes.

## Main files

| File | Purpose |
|---|---|
| `app.py` | FastAPI API server |
| `vocanote_worker.py` | Processing worker daemon |
| `vocanote_queue.py` | SQLite queue helpers |
| `vocanote_chunking.py` | Transcript/correction chunking helpers |
| `vocanote_semantic_executor.py` | Semantic extraction executor |
| `vocanote_semantic_reduce.py` | Semantic reduce/final adapter |
| `vocanote_tombstone.py` | Delete/tombstone/fencing logic |
| `vocanote_cloud.py` | Optional cloud archive helpers |
| `render_result.py` | Markdown rendering |
| `*.schema.json` | Validation schemas |

## Install sketch

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install fastapi uvicorn jsonschema openai-whisper google-api-python-client google-auth google-auth-oauthlib
```

Whisper requires `ffmpeg` on PATH.

## Run API locally

```bash
export VOCANOTE_UPLOAD_TOKEN=dev-token
uvicorn app:app --host 127.0.0.1 --port 8793
```

## Run worker

```bash
export VOCANOTE_UPLOAD_TOKEN=dev-token
export VOCANOTE_STT_ENGINE=whisper_cpp
export VOCANOTE_WHISPER_MODEL=large-v3
export VOCANOTE_WHISPER_CPP_BIN=/opt/homebrew/bin/whisper-cli
export VOCANOTE_WHISPER_CPP_MODEL=/path/to/ggml-large-v3.bin
python vocanote_worker.py
```

## Production STT engine / rollback

Production default is:

```text
VOCANOTE_STT_ENGINE=whisper_cpp
engine=whisper.cpp
model=large-v3 GGML
Metal=enabled
language=Korean
args=-t 10 -bs 5 -bo 5 -tp 0 -mc 0
```

Rollback keeps the existing OpenAI Whisper `large-v3` implementation available:

```bash
export VOCANOTE_STT_ENGINE=openai_whisper
# restart the worker after changing the service/launchd environment
```

No automatic fallback is performed. If `whisper.cpp` fails, the job enters the existing retry/failure path with stderr and exit code recorded.

For Android/uploaded `.m4a` inputs, the worker first converts the full recording to a single 16 kHz mono PCM WAV because `whisper.cpp`/miniaudio does not reliably decode the Android `.m4a` directly. This is not VAD or chunking; the full recording remains one STT input, matching the C4 benchmark input shape.

## Review focus

The current highest-value review area is closing the gap with ClovaNote-like transcript quality:

1. Improve raw STT sensitivity for quiet/noisy multi-speaker meetings.
2. Add speaker diarization or backend speaker grouping.
3. Produce speaker-paragraph transcript blocks instead of 1-second raw segments.
4. Preserve long-recording checkpoint/retry semantics while improving quality.
