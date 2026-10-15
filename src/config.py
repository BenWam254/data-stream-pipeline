import os
from pydantic import BaseModel


class Settings(BaseModel):
    redpanda_broker: str = os.getenv("REDPANDA_BROKER", "localhost:19092")
    topic: str = os.getenv("RAW_SCRAPES_TOPIC", "raw-scrapes")
    consumer_group: str = os.getenv("CONSUMER_GROUP", "data-stream-pipeline-group")
    batch_size: int = int(os.getenv("BATCH_SIZE", "50"))
    flush_interval_seconds: float = float(os.getenv("FLUSH_INTERVAL_SECONDS", "10.0"))
    output_dir: str = os.getenv("OUTPUT_DIR", "data/parquet")


settings = Settings()