from __future__ import annotations

import datetime
import hashlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FileTarget:
    exchange: str
    symbol: str
    stream: str
    date: str
    hour: int

    @property
    def key(self) -> tuple:
        return (self.exchange, self.symbol, self.stream, self.date, self.hour)


def build_file_path(
    base_dir: str,
    exchange: str,
    symbol: str,
    stream: str,
    date: str,
    hour: int,
    late_seq: int | None = None,
) -> Path:
    symbol = symbol.lower()
    name = f"hour-{hour}"
    if late_seq is not None:
        name += f".late-{late_seq}"
    name += ".jsonl.zst"
    return Path(base_dir) / exchange / symbol / stream / date / name


def sidecar_path(data_path: Path) -> Path:
    return data_path.with_suffix(data_path.suffix + ".sha256")


def compute_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def write_sha256_sidecar(data_path: Path, sc_path: Path) -> None:
    digest = compute_sha256(data_path)
    sc_path.write_text(f"{digest}  {data_path.name}\n")
