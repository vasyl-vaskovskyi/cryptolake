# CryptoPanner

Raw-first, multi-region capture pipeline for Binance USD-M Futures market data. The end product is an append-only, byte-faithful collection of per-node hourly sealed files in IONOS S3-compatible object storage.

## Documentation

- **Master specification:** [`docs/00-master-spec.md`](docs/00-master-spec.md) — the canonical project spec, reviewed end-to-end.
- **Hot-swap and WS rotation design:** [`docs/superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md`](docs/superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md) — detailed design for the make-before-break protocol used by JAR deploys and daily 24h rotations.
- **Build, test, and chaos conventions:** [`CLAUDE.md`](CLAUDE.md).

## Status

Specification: complete. Implementation: not yet started.
