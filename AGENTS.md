# Repository engineering rules

Preserve the distinction between generated text, dispatched media, and playback evidence.
Never claim live model, carrier, device, or fleet performance from fake-model tests.
No credentials, recordings, model weights, or runtime databases belong in Git.
Run the deterministic test suite before changes to session ownership, interruption, or transport code.
Redis control state is not an audio bus. Keep per-frame audio and playout state with one live owner.
All distributed state changes must be tenant-scoped and reject stale owners.
