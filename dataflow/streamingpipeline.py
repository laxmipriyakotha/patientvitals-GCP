import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

# ------------------- Environment Variables -------------------
PUBSUB_SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION", "")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "")

if not PUBSUB_SUBSCRIPTION or not BIGQUERY_TABLE:
    raise SystemExit("Missing env vars. Set PUBSUB_SUBSCRIPTION and BIGQUERY_TABLE.")

print("PUBSUB_SUBSCRIPTION =", PUBSUB_SUBSCRIPTION)
print("BIGQUERY_TABLE =", BIGQUERY_TABLE)

# ------------------- BigQuery Schema -------------------
BQ_SCHEMA = (
    "event_ts:TIMESTAMP,"
    "patient_id:INTEGER,"
    "heart_rate:INTEGER,"
    "temperature:FLOAT,"
    "bp_diastolic:INTEGER,"
    "bp_systolic:INTEGER,"
    "spo2:INTEGER,"
    "ingest_ts:TIMESTAMP"
)

# ------------------- DoFn: Parse & Validate -------------------
class ParseAndValidate(beam.DoFn):
    def process(self, message_bytes: bytes):
        ingest_ts = beam.utils.timestamp.Timestamp.now().to_rfc3339()

        try:
            raw = message_bytes.decode("utf-8")
            obj = json.loads(raw)

            # Required fields check
            required = [
                "event_ts",
                "patient_id",
                "heart_rate",
                "temperature",
                "bp_diastolic",
                "bp_systolic",
                "spo2"
            ]

            if not all(k in obj for k in required):
                return  # drop bad records

            # Type casting
            row = {
                "event_ts": obj["event_ts"],
                "patient_id": int(obj["patient_id"]),
                "heart_rate": int(obj["heart_rate"]),
                "temperature": float(obj["temperature"]),
                "bp_diastolic": int(obj["bp_diastolic"]),
                "bp_systolic": int(obj["bp_systolic"]),
                "spo2": int(obj["spo2"]),
                "ingest_ts": ingest_ts,
            }

            # ------------------- Basic Sanity Checks -------------------
            if not (0 < row["patient_id"] <= 10_000): return
            if not (30 <= row["heart_rate"] <= 220): return
            if not (85.0 <= row["temperature"] <= 110.0): return  # Fahrenheit
            if not (30 <= row["bp_diastolic"] <= 150): return
            if not (50 <= row["bp_systolic"] <= 250): return
            if not (50 <= row["spo2"] <= 100): return

            yield row

        except Exception:
            # Drop malformed messages silently
            return

# ------------------- Pipeline Runner -------------------
def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(
                subscription=PUBSUB_SUBSCRIPTION
            )
            | "Parse & Validate (Silver)" >> beam.ParDo(ParseAndValidate())
            | "Write Gold to BigQuery" >> beam.io.WriteToBigQuery(
                BIGQUERY_TABLE,
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )

# ------------------- Main -------------------
if __name__ == "__main__":
    run()
