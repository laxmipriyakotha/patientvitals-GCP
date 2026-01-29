patientvitalsproject/
├── README.md
├── architecture/
│   └── architecture.mmd
├── simulator/
│   ├── patient_vitals_simulator.py
│   ├── requirements.txt
│   └── .env.example
├── dataflow/
│   ├── streaming_pipeline.py
│   ├── requirements.txt
│   └── .env.example
└── sql/
    ├── create_dataset.sql
    └── sample_queries.sql

Commands to reorganize (Cloud Shell)
cd ~/patientvitalsproject

mkdir -p architecture simulator dataflow sql

# Move/rename simulator
mv simulator1/patient_vitals.py simulator/patient_vitals_simulator.py

# Move/rename pipeline
mv dataflow/streamingpipeline.py dataflow/streaming_pipeline.py

# (Optional) remove empty old folder
rmdir simulator1 2>/dev/null || true

2) README.md (copy/paste template)

Create this file at: ~/patientvitalsproject/README.md

# Patient Vitals Streaming Pipeline (GCP) — Pub/Sub → Dataflow → BigQuery → Power BI

A real-time healthcare streaming demo that simulates patient vitals, publishes events to Pub/Sub, processes/validates them with Apache Beam on Dataflow, and writes analytics-ready records into BigQuery for BI dashboards (Power BI).

## Architecture
Pub/Sub (Topic) → Pub/Sub (Subscription) → Dataflow (Apache Beam streaming) → BigQuery (healthcare.patient_risk_analytics) → Power BI

<img width="2089" height="732" alt="patient_vitals_architecture" src="https://github.com/user-attachments/assets/7182ca5f-602f-413f-a0a6-c4fc34254dfd" />

## Dataset (Synthetic)
Each event contains:
- event_ts (UTC ISO timestamp)
- patient_id (int)
- heart_rate (int)
- temperature (float, Fahrenheit)
- bp_systolic (int)
- bp_diastolic (int)
- spo2 (int)
- ingest_ts (Dataflow ingestion timestamp)

## Prerequisites
- GCP project with billing enabled
- APIs enabled: Pub/Sub, Dataflow, BigQuery, Cloud Storage
- Cloud Shell or local environment with:
  - Python 3.10+
  - gcloud, gsutil, bq

## Setup

### 1) Set project + create Pub/Sub resources
export PROJECT_ID="patientvitals1"
gcloud config set project "$PROJECT_ID"

export TOPIC_ID="patient_vitals_stream"
export SUB_ID="patient_vitals_stream_sub"

gcloud pubsub topics create "$TOPIC_ID" || true
gcloud pubsub subscriptions create "$SUB_ID" --topic="$TOPIC_ID" || true

2) Create BigQuery dataset
bq --location=us-central1 mk -d "${PROJECT_ID}:healthcare" || true

3) Create GCS bucket for Dataflow staging/temp
export REGION="us-central1"
export BUCKET="${PROJECT_ID}-df-$(date +%s)"

gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$BUCKET"

4) Run Dataflow streaming job
cd dataflow

export PUBSUB_SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/${SUB_ID}"
export BIGQUERY_TABLE="${PROJECT_ID}:healthcare.patient_risk_analytics"

python3 streaming_pipeline.py \
  --runner=DataflowRunner \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --worker_machine_type="e2-standard-2" \
  --num_workers=1 \
  --max_num_workers=1 \
  --staging_location="gs://$BUCKET/staging/" \
  --temp_location="gs://$BUCKET/temp/" \
  --requirements_file=requirements.txt \
  --save_main_session \
  --streaming

5) Start the simulator

In a second terminal:

cd simulator

export PROJECT_ID="patientvitals1"
export TOPIC_ID="patient_vitals_stream"
export ERROR_RATE=0
export STREAM_INTERVAL=1

python3 patient_vitals_simulator.py

Verification
Pub/Sub check (pull a few messages)
gcloud pubsub subscriptions pull patient_vitals_stream_sub \
  --project="$PROJECT_ID" --limit=5 --auto-ack

BigQuery check (row count)
bq query --use_legacy_sql=false \
"SELECT COUNT(*) AS row_count FROM \`${PROJECT_ID}.healthcare.patient_risk_analytics\`"

Power BI

Use Power BI Desktop → Get Data → Google BigQuery → select:

Project: patientvitals1

Dataset: healthcare

Table: patient_risk_analytics
Choose DirectQuery for “near real-time” visuals (or Import for faster local performance).

Enhancements (Planned)

Bronze/Silver/Gold medallion tables (BQ or GCS)

Dead-letter topic/table for invalid records

Windowed aggregations (1-min average HR, SpO2 alert counts)

Data quality metrics + alerts


---






Bronze: raw JSON table

Silver: validated table

Gold: analytics table (current)
…without making the project complicated.
