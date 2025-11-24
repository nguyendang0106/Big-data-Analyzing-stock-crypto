Demo Data Pipeline

This directory contains a lightweight, self-contained demo of a streaming-style
data pipeline implemented in pure Python (no Kafka/Spark required). It demonstrates
three logical pipeline components:

- Producer: fetches top cryptocurrency quotes from CoinGecko and publishes events.
- Processor: computes a simple moving average (SMA) per coin over a sliding window.
- Consumer: persists enriched events to `data/pipeline_output.jsonl`.

How to run

1. Activate your virtualenv (if you use one):

```bash
source venv/bin/activate
```

2. Install dependency:

```bash
pip install -r data_pipeline/requirements.txt
```

3. Run the demo:

```bash
python data_pipeline/demo_run.py
```

The demo runs 3 produce cycles (configurable in `demo_run.py`) and writes results to
`data/pipeline_output.jsonl` as JSON Lines. Use this as a minimal example to extend
with Kafka producers/consumers, Spark Streaming jobs, or Airflow orchestration.

Notes

- The demo intentionally keeps dependencies minimal: only `requests` is required.
- Adjust `interval` and `iterations` in `demo_run.py` for longer/shorter runs.
