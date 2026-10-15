import os
import time
import logging
from urllib.parse import urlparse
import polars as pl

logger = logging.getLogger("stream_batcher")


class StreamBatcher:
    """Buffers raw JSON stream payloads in memory and flushes them to disk as
    Snappy-compressed, hive-partitioned Parquet datasets using Polars.
    """

    def __init__(self, batch_size: int = 50, flush_interval: float = 10.0, output_dir: str = "data/parquet"):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.output_dir = output_dir
        self.buffer: list[dict] = []
        self.last_flush_time = time.time()

    def add_record(self, record: dict) -> bool:
        """Appends a record to the in-memory buffer.
        Returns True if the buffer threshold is met and ready for flushing.
        """
        self.buffer.append(record)
        return len(self.buffer) >= self.batch_size or (time.time() - self.last_flush_time) >= self.flush_interval

    def extract_domain(self, url: str) -> str:
        """Extracts netloc/domain from URL for partitioning."""
        try:
            parsed = urlparse(url)
            return parsed.netloc if parsed.netloc else "unknown"
        except Exception:
            return "unknown"

    def flush(self) -> int:
        """Flushes buffered records to disk as partitioned Parquet files."""
        if not self.buffer:
            return 0

        count = len(self.buffer)
        try:
            enriched_records = []
            for item in self.buffer:
                ts = item.get("timestamp", time.time())
                struct_time = time.gmtime(ts)
                enriched_item = {
                    **item,
                    "year": struct_time.tm_year,
                    "month": f"{struct_time.tm_mon:02d}",
                    "domain": self.extract_domain(item.get("url", "")),
                }
                enriched_records.append(enriched_item)

            df = pl.DataFrame(enriched_records)

            df.write_parquet(
                self.output_dir,
                use_pyarrow=True,
                pyarrow_options={"partition_cols": ["year", "month", "domain"]},
                compression="snappy",
            )

            logger.info(f"Flushed {count} records into Parquet storage under '{self.output_dir}'")
            self.buffer.clear()
            self.last_flush_time = time.time()
            return count

        except Exception as err:
            logger.error(f"Failed to flush buffer to Parquet: {err}")
            raise err