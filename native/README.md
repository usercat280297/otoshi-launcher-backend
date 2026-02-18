# Native Modules (Mandate-All Foundation)

This folder hosts native modules that back the large-scale ingest/security pipeline.

- `go/steam_crawler`: high-throughput Steam app-list crawler (JSON output).
- `c/crypto_helper`: lightweight hashing and constant-time compare helpers.
- `cpp/fs_scanner`: filesystem scan helper for verify/move/install operations.
- `asm/hash_compare`: x64 compare routine for benchmarked hot paths.

These modules are intentionally standalone so they can be built independently and wired into Python/Rust services via subprocess or FFI.
