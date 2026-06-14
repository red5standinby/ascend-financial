// AudioWorkletProcessor for reliable PCM capture on all browsers
// Buffers audio before sending to avoid tiny chunks that confuse Deepgram

class PCMProcessor extends AudioWorkletProcessor {
  constructor(options) {
    super();
    this.targetRate = options.processorOptions?.targetRate || 16000;
    this.running = true;
    this.buffer = []; // accumulate float32 samples
    this.MIN_FRAMES = 2048; // ~128ms at 16kHz — Deepgram needs decent-sized chunks
    this.port.onmessage = (e) => {
      if (e.data === 'stop') this.running = false;
    };
  }

  process(inputs, outputs, parameters) {
    if (!this.running) return true;
    const input = inputs[0];
    if (!input || !input[0] || input[0].length === 0) return true;

    let channelData = input[0];

    // Resample to target rate if different from context rate
    if (sampleRate && sampleRate !== this.targetRate) {
      channelData = this._resample(channelData, sampleRate, this.targetRate);
    }

    // Accumulate into buffer
    for (let i = 0; i < channelData.length; i++) {
      this.buffer.push(channelData[i]);
    }

    // Send when we have enough samples
    while (this.buffer.length >= this.MIN_FRAMES) {
      const frames = this.buffer.splice(0, this.MIN_FRAMES);
      const pcm = new Int16Array(frames.length);
      for (let i = 0; i < frames.length; i++) {
        const s = Math.max(-1, Math.min(1, frames[i]));
        pcm[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
      }
      this.port.postMessage(pcm.buffer, [pcm.buffer]);
    }

    return true;
  }

  _resample(input, fromRate, toRate) {
    const ratio = fromRate / toRate;
    const outLen = Math.floor(input.length / ratio);
    const out = new Float32Array(outLen);
    for (let i = 0; i < outLen; i++) {
      const pos = i * ratio;
      const idx = Math.floor(pos);
      const frac = pos - idx;
      out[i] = idx + 1 < input.length
        ? input[idx] * (1 - frac) + input[idx + 1] * frac
        : input[idx];
    }
    return out;
  }
}

registerProcessor('pcm-processor', PCMProcessor);
