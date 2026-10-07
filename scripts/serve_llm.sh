#!/usr/bin/env bash
set -euo pipefail
# Install vLLM in its own Python environment with a compatible CUDA/PyTorch stack.
# Source-aligned launch recipe: execute your GPU acceptance tests before deployment.
args=(serve "${LLM_HF_MODEL:-Qwen/Qwen3-4B-Instruct-2507}"
  --served-model-name "${LLM_MODEL:-voice-llm}"
  --host "${LLM_BIND_HOST:-127.0.0.1}" --port "${LLM_PORT:-8002}"
  --max-model-len "${LLM_CONTEXT:-8192}" --gpu-memory-utilization "${LLM_GPU_FRACTION:-0.65}"
  --enable-prefix-caching)
[[ -z "${LLM_REVISION:-}" ]] || args+=(--revision "$LLM_REVISION")
[[ -z "${LLM_API_KEY:-}" ]] || args+=(--api-key "$LLM_API_KEY")
exec vllm "${args[@]}"
