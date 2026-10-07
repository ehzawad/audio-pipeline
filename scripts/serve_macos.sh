#!/usr/bin/env bash
set -euo pipefail
# Download once with: python -m scripts.download_models --gguf
# Uses a local, manifest-recorded artifact; no implicit latest model download here.
model="${LLM_GGUF_PATH:-models/llm/Qwen3-4B-Instruct-2507-Q4_K_M.gguf}"
[[ -f "$model" ]] || { echo "Set LLM_GGUF_PATH to the Q4_K_M file recorded in models/manifest.json" >&2; exit 1; }
args=(-m "$model" --alias "${LLM_MODEL:-voice-llm}"
  --host "${LLM_BIND_HOST:-127.0.0.1}" --port "${LLM_PORT:-8002}"
  -c "${LLM_CONTEXT:-8192}" -ngl "${LLM_GPU_LAYERS:-99}")
[[ -z "${LLM_API_KEY:-}" ]] || args+=(--api-key "$LLM_API_KEY")
exec llama-server "${args[@]}"
