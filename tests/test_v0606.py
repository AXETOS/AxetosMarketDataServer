from pathlib import Path


def test_targeted_backfill_upload_is_chunked_and_acknowledged_before_completion() -> None:
    bridge = Path("bridges/mt5/Experts/AxetosMarketDataBridge.mq5").read_text(encoding="utf-8")

    assert '#property version   "1.30"' in bridge
    assert "const int upload_chunk_size = 100;" in bridge
    assert "uploading backfill chunk %d/%d" in bridge
    assert "backfill chunk %d/%d was not acknowledged" in bridge
    assert "chunk_stored + chunk_skipped != chunk_end - chunk_start" in bridge
    assert "stored_out += chunk_stored;" in bridge
    assert "skipped_out += chunk_skipped;" in bridge


def test_1435_bar_range_is_split_into_fifteen_bounded_posts() -> None:
    copied = 1435
    chunk_size = 100
    chunks = [(start, min(copied, start + chunk_size)) for start in range(0, copied, chunk_size)]

    assert len(chunks) == 15
    assert [end - start for start, end in chunks[:-1]] == [100] * 14
    assert chunks[-1][1] - chunks[-1][0] == 35
    assert sum(end - start for start, end in chunks) == 1435
