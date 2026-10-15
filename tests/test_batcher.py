import os
import shutil
import polars as pl
from src.batcher import StreamBatcher


def test_batcher_add_and_flush():
    test_output_dir = "tests/tmp_parquet"
    if os.path.exists(test_output_dir):
        shutil.rmtree(test_output_dir)

    batcher = StreamBatcher(batch_size=3, flush_interval=60.0, output_dir=test_output_dir)

    sample_event_1 = {
        "scraper_name": "test_bot",
        "url": "https://news.ycombinator.com/item?id=1",
        "status_code": 200,
        "response_time_ms": 120.5,
        "raw_content": "# Sample Article 1",
        "timestamp": 1700000000,
    }

    sample_event_2 = {
        "scraper_name": "test_bot",
        "url": "https://news.ycombinator.com/item?id=2",
        "status_code": 200,
        "response_time_ms": 95.2,
        "raw_content": "# Sample Article 2",
        "timestamp": 1700000000,
    }

    assert not batcher.add_record(sample_event_1)
    assert batcher.add_record(sample_event_2) or len(batcher.buffer) == 2

    flushed_count = batcher.flush()
    assert flushed_count == 2
    assert len(batcher.buffer) == 0

    df = pl.read_parquet(test_output_dir)
    assert df.height == 2
    assert "domain" in df.columns
    assert "news.ycombinator.com" in df["domain"].to_list()

    if os.path.exists(test_output_dir):
        shutil.rmtree(test_output_dir)