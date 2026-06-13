# Agent Voice Comms — Project State

**Updated:** 2026-06-12

## What's Built
- [x] Express/WebSocket server (port 8766)
- [x] SPA frontend with mic capture, VU meter, transcript/reply display
- [x] Deepgram streaming STT integration (nova-2, real-time)
- [x] ElevenLabs TTS streaming (eleven_turbo_v2)
- [x] Project files: README, package.json, .env.example

## Architecture
```
Browser mic → WebSocket → Deepgram STT → server/agent → ElevenLabs TTS → WebSocket → Speakers
               (binary)     (streaming)                  (streaming)        (mp3)
```

## What's Next
- [ ] Wire Chombi as the actual agent brain (replace echo reply)
- [ ] Add API keys to .env and test end-to-end
- [ ] Push-background recording (listen for interruptions)
- [ ] Multi-agent support (pick which agent to talk to)
- [ ] Add to Dashboard pipeline

## Keys Needed
- DEEPGRAM_API_KEY
- ELEVENLABS_API_KEY
