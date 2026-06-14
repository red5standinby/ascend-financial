/**
 * Agent Voice Comms — Server
 * 
 * Browser mic → WebSocket → Deepgram STT → agent → Deepgram TTS → WebSocket → Speakers
 * 
 * Designed to run on Railway (HTTP, PORT env var, TLS at edge).
 * Also runs locally with HTTPS on HTTPS_PORT (default 8767) when cert.pem exists.
 */

import 'dotenv/config';
import express from 'express';
import { createServer as createHttpServer } from 'http';
import { createServer as createHttpsServer } from 'https';
import { readFileSync, existsSync } from 'fs';
import { WebSocketServer } from 'ws';
import { createClient as createDeepgramClient } from '@deepgram/sdk';
import { Readable } from 'stream';

// ─── Config ────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 8766;
const HTTPS_PORT = process.env.HTTPS_PORT || 8767;
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY;

// ─── Services ──────────────────────────────────────────────────────────────

let deepgram;

if (DEEPGRAM_API_KEY) {
  deepgram = createDeepgramClient(DEEPGRAM_API_KEY);
}

// ─── App ───────────────────────────────────────────────────────────────────

const app = express();
app.use(express.static('public'));

// Also serve Volt Labs dashboard — accessible from the tunnel
app.use('/dashboard', express.static('../dashboard'));

// Route /team loads team.html explicitly
app.get('/team', (req, res) => {
  res.sendFile('team.html', { root: '../dashboard' });
});
app.get('/team.html', (req, res) => {
  res.sendFile('team.html', { root: '../dashboard' });
});
// Route /trend for trend-tracker
app.get('/trend', (req, res) => {
  res.sendFile('trend-tracker.html', { root: '../dashboard' });
});
app.get('/trend-tracker.html', (req, res) => {
  res.sendFile('trend-tracker.html', { root: '../dashboard' });
});
// Route dashboard root
app.get('/index.html', (req, res) => {
  res.sendFile('index.html', { root: '../dashboard' });
});

// On Railway: HTTP. Locally: HTTPS with self-signed cert.
const hasCerts = existsSync('key.pem') && existsSync('cert.pem');
const server = hasCerts
  ? createHttpsServer({ key: readFileSync('key.pem'), cert: readFileSync('cert.pem') }, app)
  : createHttpServer(app);

const wss = new WebSocketServer({ server });

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    deepgram: !!deepgram,
    tts: 'deepgram ' + !!deepgram,
  });
});

// ─── Deepgram Streaming ────────────────────────────────────────────────────
// V3 SDK: Create a live transcription connection, forward audio chunks,
// receive transcripts back.

function createDeepgramStream(onTranscript, onError) {
  if (!deepgram) return null;

  // V3 @deepgram/sdk uses listen.live()
  const dgConnection = deepgram.listen.live({
    model: 'nova-2',
    language: 'en',
    smart_format: true,
    punctuate: true,
    interim_results: true,
    encoding: 'linear16',
    sample_rate: 16000,
    channels: 1,
  });

  dgConnection.on('Metadata', (m) => {
    console.log('[dg] metadata:', JSON.stringify(m).substring(0, 200));
  });
  dgConnection.on('UtteranceEnd', () => {
    console.log('[dg] utterance end');
  });

  dgConnection.on('open', () => {
    console.log('[dg] stream open');
  });

  dgConnection.on('close', () => {
    console.log('[dg] stream closed');
  });

  dgConnection.on('error', (err) => {
    console.error('[dg] error:', err.message);
    if (onError) onError(err.message);
  });

  dgConnection.on('Results', (result) => {
    try {
      const channel = result.channel?.alternatives?.[0];
      if (!channel) return;

      const transcript = channel.transcript?.trim();
      if (!transcript) return;

      const isFinal = result.is_final;
      if (onTranscript) onTranscript(transcript, isFinal);
    } catch (err) {
      console.error('[dg] parse error:', err.message);
    }
  });

  return dgConnection;
}

// ─── TTS ────────────────────────────────────────────────────────────────────

async function streamTTS(ws, text) {
  try {
    if (deepgram) {
      return await deepgramTTS(ws, text);
    }
    console.warn('[tts] no TTS provider available');
  } catch (err) {
    console.error('[tts] error:', err.message);
  }
}

async function deepgramTTS(ws, text) {
  const result = await deepgram.speak.request(
    { text },
    { model: 'aura-orion-en', encoding: 'mp3' }
  );
  const stream = await result.getStream();
  const reader = stream.getReader();
  const chunks = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  const audio = Buffer.concat(chunks);
  if (ws.readyState === ws.OPEN && audio.length > 0) {
    ws.send(audio);
  }
}

// ─── WebSocket ─────────────────────────────────────────────────────────────

wss.on('connection', (ws) => {
  console.log('[ws] client connected');
  let dgStream = null;
  let isRecording = false;

  // Keepalive ping every 15s to prevent iOS from killing idle WS
  const keepalive = setInterval(() => {
    if (ws.readyState === ws.OPEN) {
      ws.ping();
    }
  }, 15000);

  ws.on('message', (data, isBinary) => {
    // ── Binary audio data ──
    if (isBinary) {
      console.log('[ws] audio chunk:', data.length, 'bytes');
      if (dgStream && isRecording) {
        dgStream.send(data);
      } else if (!deepgram) {
        if (!ws._echoBuffer) ws._echoBuffer = Buffer.alloc(0);
        ws._echoBuffer = Buffer.concat([ws._echoBuffer, data]);
        if (ws._echoBuffer.length > 32000) {
          ws.send(JSON.stringify({ type: 'text', text: `[echo] received ${ws._echoBuffer.length} bytes` }));
          ws._echoBuffer = null;
        }
      }
      return;
    }

    // ── JSON control messages ──
    try {
      const text = data.toString();
      console.log('[ws] text msg:', text.substring(0, 80));
      const msg = JSON.parse(text);

      switch (msg.type) {
        case 'start':
          console.log('[ws] start recording');
          isRecording = true;

          if (deepgram) {
            dgStream = createDeepgramStream(
              (transcript, isFinal) => {
                ws.send(JSON.stringify({ type: isFinal ? 'final' : 'interim', text: transcript }));
                if (isFinal) {
                  handleAgentMessage(ws, transcript);
                }
              },
              (err) => ws.send(JSON.stringify({ type: 'error', message: `STT error: ${err}` }))
            );

            if (dgStream) {
              console.log('[dg] ready for audio');
              ws.send(JSON.stringify({ type: 'status', message: 'listening' }));
            }
          } else {
            ws.send(JSON.stringify({ type: 'status', message: 'listening (echo mode)' }));
          }
          break;

        case 'end':
          console.log('[ws] stop recording');
          isRecording = false;
          if (dgStream) {
            try { dgStream.finish(); } catch {}
            dgStream = null;
          }
          break;
      }
    } catch (err) {
      console.error('[ws] json error:', err.message);
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid message' }));
    }
  });

  ws.on('close', () => {
    clearInterval(keepalive);
    console.log('[ws] client disconnected');
    if (dgStream) {
      try { dgStream.finish(); } catch {}
      dgStream = null;
    }
  });
});

// ─── Agent Brain ───────────────────────────────────────────────────────────
// Text from STT → Chombi → response text → TTS

async function handleAgentMessage(ws, text) {
  // Send interim thinking status
  ws.send(JSON.stringify({ type: 'status', message: 'thinking...' }));

  try {
    // Import and use OpenClaw's session mechanism
    const reply = await getAgentReply(text);

    // Send text reply to UI
    ws.send(JSON.stringify({ type: 'reply', text: reply }));

    // Stream TTS audio back
    if (deepgram) {
      setStatus('speaking');
      try {
        await streamTTS(ws, reply);
      } catch (err) {
        console.error('[agent] TTS failed:', err.message);
      }
      setStatus('listening');
    }
  } catch (err) {
    console.error('[agent] error:', err.message);
    ws.send(JSON.stringify({ type: 'error', message: 'Agent error: ' + err.message }));
  }
}

// ─── Agent Reply ───────────────────────────────────────────────────────────
// This function receives transcribed text and returns a spoken response.
// It runs inside the server process and uses OpenClaw's agent infrastructure.

async function getAgentReply(text) {
  // Use DeepSeek for smart, contextual responses
  if (!DEEPSEEK_API_KEY) {
    return 'No API key configured.';
  }
  try {
    const response = await fetch('https://api.deepseek.com/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + DEEPSEEK_API_KEY,
      },
      body: JSON.stringify({
        model: 'deepseek-chat',
        messages: [
          { role: 'system', content: 'You are Chombi, an AI assistant and business partner to Jaime, CEO of Volt Labs (an AI-native software studio). You work together running the company. Keep responses brief, warm, and conversational — under 2 sentences for quick replies, 3-4 max if explaining something. No markdown, no lists. Natural casual tone like a teammate.' },
          { role: 'user', content: text }
        ],
        max_tokens: 150,
        temperature: 0.7,
        stream: false,
      })
    });
    const data = await response.json();
    const content = data.choices?.[0]?.message?.content?.trim();
    return content || 'Hmm, I didn\'t quite catch that. Try again?';
  } catch (err) {
    console.error('[llm] error:', err.message);
    return 'Sorry, my brain hiccupped. Can you repeat that?';
  }
}

// ─── Status tracking ────────────────────────────────────────────────────────
let currentStatus = 'idle';

function setStatus(status) {
  currentStatus = status;
  // Broadcast to all connected clients
  wss.clients.forEach((client) => {
    if (client.readyState === client.OPEN) {
      client.send(JSON.stringify({ type: 'status', message: status }));
    }
  });
}

// ─── Start ─────────────────────────────────────────────────────────────────

const HOST = '0.0.0.0';
const listenPort = hasCerts ? HTTPS_PORT : PORT;

server.listen(listenPort, HOST, () => {
  console.log(`\n  ⚡ Agent Voice Comms  ⚡`);
  console.log(`  ───────────────────────`);
  const proto = hasCerts ? 'https' : 'http';
  console.log(`  → ${proto}://localhost:${listenPort}`);
  console.log(`  → STT: ${deepgram ? 'Deepgram ✓' : 'Deepgram ✗'}`);
  console.log(`  → TTS: Deepgram ${deepgram ? '✓' : '✗'}`);
  console.log();
});
