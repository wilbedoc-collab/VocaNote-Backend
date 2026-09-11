# VocaNote Backend Review Notes

## Immediate context

This backend is being published so other LLMs/developers can review the VocaNote pipeline. The Android client is in a separate repository:

https://github.com/wilbedoc-collab/VocaNote-Android

## Known issue from comparative testing

Private evaluation recordings showed that an established commercial transcription service captured quiet speech and proper nouns more reliably than VocaNote. Real recording titles, names, and transcript excerpts are intentionally excluded from this public repository.

Previous backend STT setting observed before Phase 1:

```text
VOCANOTE_WHISPER_MODEL=small
```

Phase 1 changes only the production default Whisper model to `large-v3`; it does not add VAD, chunking, retry, diarization, prompt changes, glossary, or schema changes.

The gap appears to be raw STT quality first, then diarization/UI grouping second.

## Current safety architecture

- Recording jobs use durable ownership, heartbeat, and stale-worker recovery.
- Artifact publication is generation-fenced and uses guarded atomic writes.
- Delete/Forget semantics use tombstones so stale workers cannot republish deleted recordings.
- Safe-edit state is isolated behind explicit feature gates; destructive trim/split remains disabled by default.
- Reconciliation is idempotent and reuses valid checkpoints rather than recomputing them.

## Review priorities

1. STT model/settings benchmark
   - Compare `small`, `medium`, `large-v3`/`large-v3-turbo` or faster-whisper equivalents.
   - Score against human/Clova reference for missing phrases, names, medical terms, and low-volume speech.

2. Audio preprocessing
   - Normalize loudness.
   - Consider VAD carefully; avoid dropping quiet speech.
   - Preserve timestamps for playback sync.

3. Speaker diarization
   - Add pyannote/WhisperX/faster-whisper+diarization evaluation.
   - Backend should emit speaker-grouped paragraph blocks for Android.

4. Segment grouping
   - Do not expose 1-second STT fragments directly as user transcript UX.
   - Build grouped transcript blocks: same speaker + short gap + sentence continuity.

5. Safety
   - Do not commit real audio/transcripts/tokens.
   - Keep deletion tombstone/fencing guarantees.
   - Keep long-recording checkpoint reuse.
