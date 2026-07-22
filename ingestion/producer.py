import argparse
import json
import os
import time
from datetime import datetime, timezone

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
STREAM = os.getenv("KINESIS_STREAM", "earthquake-stream")
S3_RAW = os.getenv("S3_RAW", "earthquake-pipeline-raw")
FEED = os.getenv(
    "USGS_FEED",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
)

kinesis = boto3.client("kinesis", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def region_key(place):
    if not place:
        return "unknown"
    parts = [p.strip() for p in place.split(",")]
    return parts[-1].lower() if parts else "unknown"


def parse_feature(f):
    p = f.get("properties") or {}
    g = f.get("geometry") or {}
    coords = g.get("coordinates") or [None, None, None]
    return {
        "id": f.get("id"),
        "mag": p.get("mag"),
        "place": p.get("place"),
        "time": p.get("time"),
        "updated": p.get("updated"),
        "type": p.get("type"),
        "lon": coords[0],
        "lat": coords[1],
        "depth": coords[2],
        "region": region_key(p.get("place")),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_usgs():
    r = requests.get(FEED, timeout=30)
    r.raise_for_status()
    return r.json().get("features") or []


def put_kinesis(event):
    pk = event.get("region") or event.get("id") or "x"
    kinesis.put_record(
        StreamName=STREAM,
        Data=json.dumps(event).encode("utf-8"),
        PartitionKey=str(pk)[:256],
    )


def write_s3(event):
    eid = event.get("id") or str(int(time.time() * 1000))
    key = f"data/{eid}.json"
    s3.put_object(
        Bucket=S3_RAW,
        Key=key,
        Body=json.dumps(event).encode("utf-8"),
        ContentType="application/json",
    )


def run(rate, once, dual_s3):
    seen = set()
    delay = 1.0 / rate if rate > 0 else 0
    while True:
        features = fetch_usgs()
        n = 0
        for f in features:
            ev = parse_feature(f)
            eid = ev.get("id")
            if eid in seen:
                continue
            seen.add(eid)
            put_kinesis(ev)
            if dual_s3:
                write_s3(ev)
            n += 1
            if delay:
                time.sleep(delay)
        print(f"sent={n} total_seen={len(seen)} rate={rate}/s")
        if once:
            break
        time.sleep(30)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=float(os.getenv("EVENTS_PER_SEC", "5")))
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--no-s3", action="store_true")
    args = ap.parse_args()
    run(args.rate, args.once, dual_s3=not args.no_s3)
