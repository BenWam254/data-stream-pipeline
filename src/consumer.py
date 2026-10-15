import json
import logging
import asyncio
from aiokafka import AIOKafkaConsumer
from src.config import settings
from src.batcher import StreamBatcher

logger = logging.getLogger("stream_consumer")


async def start_consumer(batcher: StreamBatcher):
    """Kafka consumer loop subscribing to Redpanda topic and piping records to StreamBatcher."""
    consumer = AIOKafkaConsumer(
        settings.topic,
        bootstrap_servers=settings.redpanda_broker,
        group_id=settings.consumer_group,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info(f"Subscribed to topic '{settings.topic}' on broker '{settings.redpanda_broker}'")

    try:
        while True:
            result = await consumer.getmany(timeout_ms=1000, max_records=20)
            for tp, messages in result.items():
                for msg in messages:
                    payload = msg.value
                    should_flush = batcher.add_record(payload) #type:ignore
                    if should_flush:
                        batcher.flush()

            if batcher.buffer and (asyncio.get_event_loop().time() - batcher.last_flush_time) >= settings.flush_interval_seconds:
                batcher.flush()

    except asyncio.CancelledError:
        logger.info("Consumer loop cancelled. Flushing remaining buffer...")
        batcher.flush()
    finally:
        await consumer.stop()