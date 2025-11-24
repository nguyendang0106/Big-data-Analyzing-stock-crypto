import json
from pathlib import Path

OUTPUT_PATH = Path("data/pipeline_output.jsonl")


def consume(output_queue):
    """Consumer that reads enriched events and appends them to JSONL file."""
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("a", encoding="utf-8") as fh:
        while True:
            item = output_queue.get()
            if item is None:
                print("[Consumer] End-of-stream received. Exiting.")
                break
            fh.write(json.dumps(item, ensure_ascii=False) + "\n")
            fh.flush()
            print(f"[Consumer] Wrote {item.get('id')} at {item.get('timestamp')}")
