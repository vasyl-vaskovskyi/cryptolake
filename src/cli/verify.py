from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from pathlib import Path

import click
import orjson
import zstandard as zstd

_REQUIRED_DATA_FIELDS = {
    "v", "type", "exchange", "symbol", "stream", "received_at",
    "exchange_ts", "collector_session_id", "session_seq", "raw_text",
    "raw_sha256", "_topic", "_partition", "_offset",
}
_REQUIRED_GAP_FIELDS = {
    "v", "type", "exchange", "symbol", "stream", "received_at",
    "collector_session_id", "session_seq", "gap_start_ts", "gap_end_ts",
    "reason", "detail", "_topic", "_partition", "_offset",
}


def verify_checksum(data_path: Path, sidecar_path: Path) -> list[str]:
    """Compare SHA-256 of data file against its .sha256 sidecar."""
    if not sidecar_path.exists():
        return [f"Sidecar not found: {sidecar_path}"]
    expected = sidecar_path.read_text().strip().split()[0]
    h = hashlib.sha256()
    with open(data_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    if h.hexdigest() != expected:
        return [f"Checksum mismatch for {data_path.name}"]
    return []


def verify_envelopes(envelopes: list[dict]) -> list[str]:
    """Validate required fields and raw_sha256 integrity for each envelope."""
    errors: list[str] = []
    for i, env in enumerate(envelopes):
        req = _REQUIRED_GAP_FIELDS if env.get("type") == "gap" else _REQUIRED_DATA_FIELDS
        missing = req - set(env.keys())
        if missing:
            errors.append(f"Line {i}: missing fields: {sorted(missing)}")
            continue
        if env.get("type") == "data":
            actual = hashlib.sha256(env["raw_text"].encode()).hexdigest()
            if actual != env["raw_sha256"]:
                errors.append(
                    f"Line {i}: raw_sha256 mismatch at offset {env.get('_offset')}"
                )
    return errors


def check_duplicate_offsets(envelopes: list[dict]) -> list[str]:
    """Find duplicate (topic, partition, offset) tuples."""
    seen: set[tuple] = set()
    errors: list[str] = []
    for env in envelopes:
        key = (env.get("_topic"), env.get("_partition"), env.get("_offset"))
        if key in seen:
            errors.append(f"Duplicate broker record: {key}")
        seen.add(key)
    return errors


def report_gaps(envelopes: list[dict]) -> list[dict]:
    """Return all gap envelopes from a list of envelopes."""
    return [e for e in envelopes if e.get("type") == "gap"]


def decompress_and_parse(file_path: Path) -> list[dict]:
    """Decompress a .jsonl.zst file and parse each line as JSON."""
    dctx = zstd.ZstdDecompressor()
    with open(file_path, "rb") as f:
        data = dctx.stream_reader(f).read()
    return [orjson.loads(line) for line in data.strip().split(b"\n") if line]


def verify_depth_replay(
    depth_envelopes: list[dict],
    snapshot_envelopes: list[dict],
    gap_envelopes: list[dict],
) -> list[str]:
    """Verify depth diffs are anchored by snapshots with valid pu chains.

    Gap records excuse breaks in the chain.
    Runs an independent pu-chain walk per symbol to avoid cross-symbol
    interference.
    """
    errors: list[str] = []
    if not depth_envelopes:
        return errors

    depth_by_sym: dict[str, list] = defaultdict(list)
    snap_by_sym: dict[str, list] = defaultdict(list)

    for e in depth_envelopes:
        depth_by_sym[e["symbol"]].append(e)
    for e in snapshot_envelopes:
        snap_by_sym[e["symbol"]].append(e)

    gap_by_sym_stream: dict[tuple[str, str], list] = defaultdict(list)
    for g in gap_envelopes:
        key = (g.get("symbol", ""), g.get("stream", ""))
        gap_by_sym_stream[key].append(g)

    for symbol in depth_by_sym:
        sym_depths = sorted(
            depth_by_sym[symbol], key=lambda e: e["received_at"]
        )
        sym_snaps = sorted(
            snap_by_sym.get(symbol, []), key=lambda e: e["received_at"]
        )
        sym_depth_gaps = (
            gap_by_sym_stream.get((symbol, "depth"), [])
            + gap_by_sym_stream.get((symbol, "depth_snapshot"), [])
        )
        gap_windows = [
            (g["gap_start_ts"], g["gap_end_ts"]) for g in sym_depth_gaps
        ]

        def _in_gap(received_at: int) -> bool:
            return any(s <= received_at <= e for s, e in gap_windows)

        snap_idx = 0
        synced = False
        last_u: int | None = None
        sync_lid: int | None = None

        for env in sym_depths:
            raw = orjson.loads(env["raw_text"])
            U = raw.get("U", 0)
            u = raw.get("u", 0)
            pu = raw.get("pu", 0)

            # Advance snapshot pointer: consume all snapshots received
            # before or at the same time as this diff.
            while (
                snap_idx < len(sym_snaps)
                and sym_snaps[snap_idx]["received_at"] <= env["received_at"]
            ):
                snap_raw = orjson.loads(sym_snaps[snap_idx]["raw_text"])
                sync_lid = snap_raw["lastUpdateId"]
                synced = True
                last_u = None
                snap_idx += 1

            if not synced:
                if not _in_gap(env["received_at"]):
                    errors.append(
                        f"[{symbol}] Depth diff at received_at "
                        f"{env['received_at']} has no preceding snapshot"
                    )
                continue

            # First diff after a snapshot must span the sync point.
            if last_u is None:
                if not (U <= sync_lid + 1 <= u):  # type: ignore[operator]
                    if not _in_gap(env["received_at"]):
                        errors.append(
                            f"[{symbol}] First diff after snapshot does not "
                            f"span sync point: lastUpdateId={sync_lid}, "
                            f"U={U}, u={u} at received_at "
                            f"{env['received_at']}"
                        )
                    continue
                last_u = u
                continue

            # Subsequent diffs: pu must equal previous u.
            if pu != last_u:
                if not _in_gap(env["received_at"]):
                    errors.append(
                        f"[{symbol}] pu chain break at received_at "
                        f"{env['received_at']}: expected pu={last_u}, "
                        f"got pu={pu}"
                    )
            last_u = u

    return errors


def generate_manifest(base_dir: Path, exchange: str, date: str) -> dict:
    """Generate a manifest summarizing archive contents for a date."""
    manifest: dict = {"date": date, "exchange": exchange, "symbols": {}}
    exchange_dir = base_dir / exchange
    if not exchange_dir.exists():
        return manifest

    for symbol_dir in sorted(exchange_dir.iterdir()):
        if not symbol_dir.is_dir():
            continue
        symbol = symbol_dir.name
        manifest["symbols"][symbol] = {"streams": {}}
        for stream_dir in sorted(symbol_dir.iterdir()):
            if not stream_dir.is_dir():
                continue
            date_dir = stream_dir / date
            if not date_dir.exists():
                continue
            hours: list[int] = []
            record_count = 0
            gaps_list: list[dict] = []
            for f in sorted(date_dir.glob("hour-*.jsonl.zst")):
                hour_str = f.name.split(".")[0].replace("hour-", "")
                try:
                    hours.append(int(hour_str))
                except ValueError:
                    pass
                try:
                    envs = decompress_and_parse(f)
                    record_count += len(envs)
                    for env in envs:
                        if env.get("type") == "gap":
                            gaps_list.append({
                                "symbol": env.get("symbol"),
                                "reason": env.get("reason"),
                                "gap_start_ts": env.get("gap_start_ts"),
                                "gap_end_ts": env.get("gap_end_ts"),
                            })
                except Exception:
                    pass  # Skip unreadable/corrupt files; they will surface via checksum errors
            if hours:
                manifest["symbols"][symbol]["streams"][stream_dir.name] = {
                    "hours": sorted(set(hours)),
                    "record_count": record_count,
                    "gaps": gaps_list,
                }
    return manifest


@click.group()
def cli():
    """CryptoLake data verification CLI."""


@cli.command()
@click.option("--date", required=True, help="Date to verify (YYYY-MM-DD)")
@click.option("--base-dir", default="/data", help="Archive base directory")
@click.option("--exchange", default=None, help="Filter by exchange")
@click.option("--symbol", default=None, help="Filter by symbol")
@click.option("--stream", default=None, help="Filter by stream")
@click.option("--full", is_flag=True, help="Full verification including cross-file dedup")
@click.option(
    "--repair-checksums", is_flag=True,
    help="Generate missing .sha256 sidecars",
)
def verify(date, base_dir, exchange, symbol, stream, full, repair_checksums):
    """Verify archive integrity for a date."""
    base = Path(base_dir)
    all_errors: list[str] = []
    all_gaps: list[dict] = []
    all_envelopes: list[dict] = []

    files = sorted(base.rglob(f"*/{date}/hour-*.jsonl.zst"))
    if not files:
        click.echo(f"No archive files found for date {date} in {base_dir}")
        return

    for data_path in files:
        parts = data_path.relative_to(base).parts
        if len(parts) < 4:
            continue
        file_exchange, file_symbol, file_stream = parts[0], parts[1], parts[2]
        if exchange and file_exchange != exchange:
            continue
        if symbol and file_symbol != symbol:
            continue
        if stream and file_stream != stream:
            continue

        click.echo(f"Verifying: {data_path.relative_to(base)}")

        sc = Path(str(data_path) + ".sha256")
        if repair_checksums and not sc.exists():
            from src.writer.file_rotator import write_sha256_sidecar

            write_sha256_sidecar(data_path, sc)
            click.echo(f"  Repaired: {sc.name}")
        checksum_errors = verify_checksum(data_path, sc)
        all_errors.extend(checksum_errors)

        try:
            envelopes = decompress_and_parse(data_path)
        except Exception as e:
            all_errors.append(f"Decompression failed: {data_path} - {e}")
            continue

        all_errors.extend(verify_envelopes(envelopes))
        all_errors.extend(check_duplicate_offsets(envelopes))
        all_gaps.extend(report_gaps(envelopes))

        if full:
            all_envelopes.extend(envelopes)

    if full and all_envelopes:
        all_errors.extend(check_duplicate_offsets(all_envelopes))

    if full and all_envelopes:
        depth_envs = [
            e for e in all_envelopes
            if e.get("stream") == "depth" and e.get("type") == "data"
        ]
        snap_envs = [
            e for e in all_envelopes
            if e.get("stream") == "depth_snapshot" and e.get("type") == "data"
        ]
        gap_envs = [e for e in all_envelopes if e.get("type") == "gap"]
        all_errors.extend(verify_depth_replay(depth_envs, snap_envs, gap_envs))

    click.echo(f"\n{'=' * 60}")
    click.echo(f"Verification complete for {date}")
    click.echo(f"Files checked: {len(files)}")
    if all_errors:
        click.echo(f"\nERRORS ({len(all_errors)}):")
        for err in all_errors:
            click.echo(f"  - {err}")
    else:
        click.echo("Errors: 0")
    if all_gaps:
        click.echo(f"\nGAPS ({len(all_gaps)}):")
        for gap in all_gaps:
            click.echo(
                f"  - {gap.get('symbol')}/{gap.get('stream')}: "
                f"{gap.get('reason')} ({gap.get('detail', '')})"
            )
    else:
        click.echo("Gaps: 0")
    if all_errors:
        raise SystemExit(1)


@cli.command()
@click.option("--date", required=True, help="Date (YYYY-MM-DD)")
@click.option("--base-dir", default="/data", help="Archive base directory")
@click.option("--exchange", default="binance", help="Exchange name")
def manifest(date, base_dir, exchange):
    """Generate manifest.json for a date directory."""
    base = Path(base_dir)
    m = generate_manifest(base, exchange, date)
    exchange_dir = base / exchange
    if exchange_dir.exists():
        for symbol_dir in exchange_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            for stream_dir in symbol_dir.iterdir():
                date_dir = stream_dir / date
                if date_dir.exists():
                    manifest_path = date_dir / "manifest.json"
                    manifest_path.write_text(json.dumps(m, indent=2) + "\n")
                    click.echo(f"Written: {manifest_path}")
    click.echo(json.dumps(m, indent=2))
