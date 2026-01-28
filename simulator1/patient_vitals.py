"""
Patient Vitals Pub/Sub Simulator (Best-of-both)

Combines:
- Code 1: .env support, configurable patients/interval/error injection, random patient IDs
- Code 2: publish confirmation (future.result), fail-fast env validation, cleaner timestamps

Features:
- Publishes JSON vitals to a Pub/Sub topic
- Error injection modes (missing_field / null_field / negative_value / out_of_range / bad_type)
- Robust publish logging + surfaced failures
- Accepts either:
   * PROJECT_ID + TOPIC_ID (topic short name), OR
   * PUBSUB_TOPIC (full topic path: projects/<project>/topics/<topic>)

Env vars (.env supported):
  PROJECT_ID=your-gcp-project               (or GCP_PROJECT)
  TOPIC_ID=your-topic-name                 (or PUBSUB_TOPIC full path)
  PUBSUB_TOPIC=projects/.../topics/...     (optional alternative)
  PATIENT_COUNT=30
  STREAM_INTERVAL=1
  ERROR_RATE=0.1
  ERROR_MODE=mixed   # mixed|none|missing_field|null_field|negative_value|out_of_range|bad_type
  MISSING_FIELD_STYLE=delete  # delete|null
"""

import os
import json
import time
import random
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from google.cloud import pubsub_v1

# Optional .env support (safe if python-dotenv isn't installed)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


def _get_env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    try:
        return int(val)
    except ValueError as e:
        raise SystemExit(f"Invalid int for {name}={val!r}") from e


def _get_env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    try:
        return float(val)
    except ValueError as e:
        raise SystemExit(f"Invalid float for {name}={val!r}") from e


def resolve_topic_path(publisher: pubsub_v1.PublisherClient) -> str:
    """
    Resolve the Pub/Sub topic path from environment variables.
    Supports:
      - PUBSUB_TOPIC as full path: projects/<project>/topics/<topic>
      - or PROJECT_ID + TOPIC_ID (or GCP_PROJECT + PUBSUB_TOPIC as topic name)
    """
    project_id = os.getenv("PROJECT_ID") or os.getenv("GCP_PROJECT") or ""
    topic_id = os.getenv("TOPIC_ID") or os.getenv("PUBSUB_TOPIC") or ""
    full_topic = os.getenv("PUBSUB_TOPIC_FULL") or ""  # optional explicit

    # If PUBSUB_TOPIC already looks like a full path, use it.
    if topic_id.startswith("projects/") and "/topics/" in topic_id:
        return topic_id

    # If an explicit full path is provided, use it.
    if full_topic.startswith("projects/") and "/topics/" in full_topic:
        return full_topic

    if not project_id or not topic_id:
        raise SystemExit(
            "Missing topic configuration.\n"
            "Set either:\n"
            "  - PUBSUB_TOPIC=projects/<project>/topics/<topic>\n"
            "OR\n"
            "  - PROJECT_ID (or GCP_PROJECT) AND TOPIC_ID (topic short name)\n"
            f"Current values: PROJECT_ID={project_id!r}, TOPIC_ID/PUBSUB_TOPIC={topic_id!r}"
        )

    return publisher.topic_path(project_id, topic_id)


def gen_vitals(patient_id: str) -> Dict[str, Any]:
    """
    Generate realistic-ish vitals.
    """
    # Common ranges (not medical advice; just simulation)
    heart_rate = random.randint(55, 130)              # bpm
    temperature = round(random.uniform(96.5, 102.5), 1)  # F
    bp_systolic = random.randint(90, 160)            # mmHg
    bp_diastolic = random.randint(55, 110)           # mmHg
    spo2 = random.randint(88, 100)                   # %

    return {
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "patient_id": patient_id,
        "heart_rate": heart_rate,
        "temperature": temperature,
        "bp_systolic": bp_systolic,
        "bp_diastolic": bp_diastolic,
        "spo2": spo2,
    }


def inject_error(
    record: Dict[str, Any],
    error_mode: str,
    missing_field_style: str,
) -> Dict[str, Any]:
    """
    Inject an error into the record based on the chosen mode.
    - missing_field: deletes a required field (or sets it null, depending on MISSING_FIELD_STYLE)
    - null_field: sets a random field to None
    - negative_value: sets heart_rate to -1
    - out_of_range: sets spo2 to 150
    - bad_type: sets bp_systolic to a string
    """
    if error_mode == "none":
        return record

    modes = ["missing_field", "null_field", "negative_value", "out_of_range", "bad_type"]
    mode = random.choice(modes) if error_mode == "mixed" else error_mode

    required_fields = ["patient_id", "event_ts", "heart_rate", "temperature", "bp_systolic", "bp_diastolic", "spo2"]

    if mode == "missing_field":
        field = random.choice(required_fields)
        if missing_field_style == "delete":
            record.pop(field, None)
        else:
            record[field] = None

    elif mode == "null_field":
        field = random.choice(required_fields)
        record[field] = None

    elif mode == "negative_value":
        record["heart_rate"] = -1

    elif mode == "out_of_range":
        record["spo2"] = 150

    elif mode == "bad_type":
        record["bp_systolic"] = "one-sixty"

    return record


def publish_message(
    publisher: pubsub_v1.PublisherClient,
    topic_path: str,
    payload: Dict[str, Any],
    timeout_sec: int = 30,
) -> str:
    """
    Publish message and block until acked to surface errors immediately.
    Returns message_id.
    """
    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    return future.result(timeout=timeout_sec)


def main() -> None:
    patient_count = _get_env_int("PATIENT_COUNT", 30)
    stream_interval = float(os.getenv("STREAM_INTERVAL", "1"))
    error_rate = _get_env_float("ERROR_RATE", 0.1)
    error_mode = (os.getenv("ERROR_MODE", "mixed") or "mixed").strip().lower()
    missing_field_style = (os.getenv("MISSING_FIELD_STYLE", "delete") or "delete").strip().lower()

    if patient_count <= 0:
        raise SystemExit("PATIENT_COUNT must be > 0")
    if stream_interval <= 0:
        raise SystemExit("STREAM_INTERVAL must be > 0")
    if not (0.0 <= error_rate <= 1.0):
        raise SystemExit("ERROR_RATE must be between 0.0 and 1.0")
    if error_mode not in {"mixed", "none", "missing_field", "null_field", "negative_value", "out_of_range", "bad_type"}:
        raise SystemExit("ERROR_MODE must be one of: mixed|none|missing_field|null_field|negative_value|out_of_range|bad_type")
    if missing_field_style not in {"delete", "null"}:
        raise SystemExit("MISSING_FIELD_STYLE must be delete or null")

    publisher = pubsub_v1.PublisherClient()
    topic_path = resolve_topic_path(publisher)

    # Generate patient IDs like P001..PNNN
    patient_ids = list(range(1, patient_count + 1))


    print("Starting patient vitals simulator...")
    print(f"Topic: {topic_path}")
    print(f"Patients: {patient_count} | Interval: {stream_interval}s | Error rate: {error_rate} | Error mode: {error_mode}")
    print("Press Ctrl+C to stop.\n")

    while True:
        try:
            patient_id = random.choice(patient_ids)
            vitals = gen_vitals(patient_id)

            # Inject error based on ERROR_RATE
            if random.random() < error_rate:
                vitals = inject_error(vitals, error_mode=error_mode, missing_field_style=missing_field_style)
                vitals["_is_error"] = True  # helpful flag for downstream filters
            else:
                vitals["_is_error"] = False

            msg_id = publish_message(publisher, topic_path, vitals, timeout_sec=30)
            print(f"Published msg_id={msg_id} payload={json.dumps(vitals)}")

            time.sleep(stream_interval)

        except KeyboardInterrupt:
            print("\nStopping simulator.")
            break
        except Exception as e:
            # Surface errors clearly; keep running to avoid losing the stream
            print(f"[ERROR] Publish failed: {type(e).__name__}: {e}")
            time.sleep(min(stream_interval, 5))


if __name__ == "__main__":
    main()
