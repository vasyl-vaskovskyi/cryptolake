import zstandard as zstd


class TestZstdCompressor:
    def test_compress_single_frame_round_trip(self):
        from src.writer.compressor import ZstdFrameCompressor

        comp = ZstdFrameCompressor(level=3)
        lines = [b'{"test": 1}\n', b'{"test": 2}\n']
        frame = comp.compress_frame(lines)

        # Decompress and verify
        dctx = zstd.ZstdDecompressor()
        decompressed = dctx.decompress(frame)
        assert decompressed == b'{"test": 1}\n{"test": 2}\n'

    def test_compress_empty_input(self):
        from src.writer.compressor import ZstdFrameCompressor

        comp = ZstdFrameCompressor(level=3)
        frame = comp.compress_frame([])
        assert frame == b""  # No data = no frame

    def test_multiple_frames_concatenated(self):
        """Zstd supports concatenated frames — each flush produces a complete frame."""
        from src.writer.compressor import ZstdFrameCompressor

        comp = ZstdFrameCompressor(level=3)
        frame1 = comp.compress_frame([b'{"batch": 1}\n'])
        frame2 = comp.compress_frame([b'{"batch": 2}\n'])
        combined = frame1 + frame2

        # Full decompression of concatenated frames
        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(combined)
        decompressed = reader.read()
        assert decompressed == b'{"batch": 1}\n{"batch": 2}\n'

    def test_frame_is_complete_zstd(self):
        """Each frame must be independently decompressible."""
        from src.writer.compressor import ZstdFrameCompressor

        comp = ZstdFrameCompressor(level=3)
        frame = comp.compress_frame([b'{"x": 1}\n'])

        dctx = zstd.ZstdDecompressor()
        result = dctx.decompress(frame)
        assert result == b'{"x": 1}\n'

    def test_configurable_compression_level(self):
        from src.writer.compressor import ZstdFrameCompressor

        comp1 = ZstdFrameCompressor(level=1)
        comp9 = ZstdFrameCompressor(level=9)
        data = [b'{"key": "value"}\n'] * 100
        frame1 = comp1.compress_frame(data)
        frame9 = comp9.compress_frame(data)
        # Higher level should compress better (or at least equal)
        assert len(frame9) <= len(frame1)
