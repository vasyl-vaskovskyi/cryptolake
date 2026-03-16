from __future__ import annotations

import zstandard as zstd


class ZstdFrameCompressor:
    """Produces complete, independent zstd frames for crash-safe file writes.

    Each call to compress_frame() returns a self-contained zstd frame.
    Concatenating multiple frames produces a valid zstd file (per spec).
    On crash recovery, the file can be truncated at any frame boundary.
    """

    def __init__(self, level: int = 3):
        self._cctx = zstd.ZstdCompressor(level=level)

    def compress_frame(self, lines: list[bytes]) -> bytes:
        if not lines:
            return b""
        data = b"".join(lines)
        return self._cctx.compress(data)
