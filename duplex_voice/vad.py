"""One private recurrent state per call, one shared read-only ONNX inference session."""
from __future__ import annotations
import numpy as np


class SileroFactory:
    def __init__(self, filename: str):
        import onnxruntime as ort
        opts = ort.SessionOptions()
        opts.inter_op_num_threads = 1
        opts.intra_op_num_threads = 1
        self.session = ort.InferenceSession(filename, sess_options=opts,
                                           providers=["CPUExecutionProvider"])
        names = {i.name for i in self.session.get_inputs()}
        if names != {"input", "state", "sr"}:
            raise ValueError(f"Expected streaming Silero ONNX input/state/sr, got {names}")

    def make(self):
        return SileroVAD(self.session)


class SileroVAD:
    def __init__(self, session):
        self.session = session
        self.state = np.zeros((2, 1, 128), np.float32)
        self.context = np.zeros((1, 64), np.float32)

    def __call__(self, samples: np.ndarray) -> float:
        if samples.shape != (512,):
            raise ValueError("Silero streaming VAD requires 512 mono samples at 16 kHz")
        x = np.concatenate((self.context, samples.reshape(1, -1)), axis=1)
        prob, self.state = self.session.run(None, {
            "input": x, "state": self.state, "sr": np.array(16000, np.int64)})
        self.context = x[:, -64:].copy()
        return float(prob.reshape(-1)[0])


class EnergyVAD:
    """Deterministic test/demo signal detector. NOT a production speech recognizer."""
    def __call__(self, samples: np.ndarray) -> float:
        return 0.99 if np.sqrt(np.mean(samples * samples)) > 0.025 else 0.01
