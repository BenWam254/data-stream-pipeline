# data-stream-pipeline

A high-throughput event-drive consumer pipeline engineered to consume raw JSON payloads from message brokers, apply zero-copy transformations, and persist partitioned Parquet data lakes.

Built with **Polars** lazy frames for memory-efficient stream processing and **PyArrow** for columnar storage operations, `data-stream-pipeline` handles stream consumer efforts, batching windows, schema casting, and date/category partioning.

--- 

## Features

 - **Stream Consumer Integration:** Subscribes to Kafka/Redpanda topic brokers for continuous event stream ingestion.
 - **Zero-Copy Transformations:** Applies lazy frame filter, cast, and cleanup operations in Rust before memory allocation.
 - **Partitioned Parquet Lake Writer:** Automatically organizes storage using date and category directory (`YYYY/MM/DD`).
 - **Configurable Batch Windows:** Triggers disk flushes based on configurable record volume thresholds or time window intervals.
 - **Graceful Offset Recovery:** Manages consumer group commit offsets to guarantee at-least-once processing semantics.

 ---

 ## Tech Stack

 - **Processing Engine:** [Polars](https://pola.rs/)
 - **Columnar Storage:** [PyArrow](https://arrow.apache.org/docs/python/)
 - **Message Broker:** [Redpanda](https://redpanda.com/) / [Apache Kafka](https://kafka.apache.org/)
 - **Package Manager:** [`uv`](https://github.com/astral-sh/uv)
 - **Language:** Python 3.12+

 --- 

 ## Project Structure

 ```text
data-stream-pipeline/
├── pipeline/
│   ├── __init__.py        # Package initialization
│   ├── consumer.py        # Kafka/Redpanda stream consumer loop
│   ├── transformer.py     # Polars lazy frame transformation logic
│   ├── writer.py          # Partitioned Parquet lake storage writer
│   └── config.py          # Broker & storage settings parser
├── tests/                 # Pytest suite for stream transformations
├── .env.example           # Broker & directory configuration template
├── .gitignore             # Git ignore configurations
├── CHANGELOG.md           # Version release history
└── README.md              # Documentation
```

## Getting Started

1. #### Prerequisites
Ensure you have Python 3.12+ and `uv` installed on your machine.

If you don't have `uv` installed, run:
```bash
# macOS/Linux
curl -LsSf [https://astral.sh/uv/install.sh](https://astral.sh/uv/install.sh) | sh

# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm [https://astral.sh/uv/install.ps1](https://astral.sh/uv/install.ps1) | iex"
```

2. #### Clone the Repository
```bash
git clone [https://github.com/BenWam254/data-stream-pipeline.git](https://github.com/BenWam254/data-stream-pipeline.git)
cd data-stream-pipeline
```

3. #### Environment Setup
Create an `.env` file in the root directory:

```bash
cp .env.example .env
```

4. #### Install Dependencies
Install dependencies into an isolated virtual environment using `uv`:

```bash
uv sync
```

5. #### Usage
#### Run Stream Consumer Pipeline
Launch the consumer loop to ingest messages and write partitioned Parquet files:

```bash
uv run python -m pipeline.consumer
```

#### Inspect Generated Parquet Lake
Query local Parquet files using Polars:

```bash
uv run python -c "import polars as pl; print(pl.scan_parquet('data/**/*.parquet').collect())"
```

## License
Distributed under the MIT License. See `LICENSE` for more information.