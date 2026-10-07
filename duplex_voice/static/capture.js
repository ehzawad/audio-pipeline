class Capture extends AudioWorkletProcessor {
  constructor() { super(); this.size = Math.round(sampleRate/50); this.buf = new Int16Array(this.size); this.i = 0; }
  process(inputs, outputs) {
    const x = inputs[0]?.[0];
    if (x) for (let j=0; j<x.length; j++) {
      this.buf[this.i++] = Math.max(-32768, Math.min(32767, Math.round(x[j]*32768)));
      if (this.i === this.size) { const b = this.buf.buffer; this.port.postMessage(b,[b]); this.buf = new Int16Array(this.size); this.i=0; }
    }
    // Never monitor the microphone directly through the speakers.
    if(outputs[0]) for(const ch of outputs[0]) ch.fill(0);
    return true;
  }
}
registerProcessor('duplex-capture', Capture);
