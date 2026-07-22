# Scalable Real-Time Earthquake Monitoring and Seismic Risk Analytics using AWS Lambda Architecture

## Dataset

| Field | Value |
|-------|--------|
| Name | USGS Real-Time Earthquake GeoJSON Feed |
| Link | https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson |
| Access | Free, no API key; polled and replayed at a controlled rate |

## Real-time questions

- Which regions have the most events in the **last 5 minutes**?
- What is the rolling window average magnitude by region?
- How do live counts compare to the full-history batch baseline (seismic risk context)?

## Why Lambda architecture

| Layer | Role |
|-------|------|
| Batch | Accurate full-history regional stats (correctness) |
| Speed | Low-latency sliding-window views (freshness) |
| Serving | Merge batch + speed for queries |

Batch-only is too stale for swarms; stream-only lacks multi-day baselines for risk context.

## Contributors

| Name | GitHub |
|------|--------|
| Venkat | srivenkatborab |
| Aakarsha | kaakarsha |

## Architecture

```
USGS all_hour.geojson
        |
        v
 Python producer (boto3) --rate control-->
        |
        v
 Kinesis Data Streams (shards >= 2)
        |
   +----+----+
   |         |
   v         v
 Batch      Speed
 S3 raw     Sliding window (5 min)
 EMR Spark  -> DynamoDB top-N + avg mag
 -> Parquet
   |         |
 Athena   DynamoDB
   +----+----+
        v
 Serving merge (CLI)
        |
 EMR Managed Scaling (min 2 / max 8)
 scale-out cooldown ~60-120s, scale-in ~300s
```

| Component | AWS / tool |
|-----------|------------|
| Ingestion | Kinesis + Python producer |
| Batch | EMR PySpark, S3 Parquet, Athena |
| Speed | Kinesis consumer, sliding window, DynamoDB |
| Serving | Athena + DynamoDB merge |
| Auto-scale | EMR Managed Scaling |

## Setup

```bash
pip install -r requirements.txt
cp ../config/.env.example ../config/.env
```

## Run

```bash
python infrastructure/setup.py
python ingestion/producer.py --rate 5
python speed/consumer.py
python batch/submit.py
python serving/query.py
python benchmarks/benchmark.py --mode all
python -m unittest tests/test_window.py
python infrastructure/teardown.py
```

## Repo

https://github.com/scalablecloud/lambda-earthquake-analytics
