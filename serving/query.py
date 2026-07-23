import os
from collections import defaultdict

import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
DYNAMO = os.getenv("DYNAMO_TABLE", "earthquake-speed-view")
ATHENA_DB = os.getenv("ATHENA_DB", "earthquake_pipeline")
ATHENA_TBL = os.getenv("ATHENA_TABLE", "batch_view")
S3_OUT = os.getenv("S3_ATHENA_OUT", "s3://earthquake-pipeline-batch/athena-results/")
TOP_N = int(os.getenv("TOP_N", "10"))

table = boto3.resource("dynamodb", region_name=REGION).Table(DYNAMO)


def get_speed_view():
    results = []
    for rank in range(1, TOP_N + 1):
        item = table.get_item(Key={"pk": f"speed#{rank}"}).get("Item")
        if item:
            results.append((item["region"], int(item["count"])))
    return results


def get_batch_view():
    try:
        from pyathena import connect

        cur = connect(s3_staging_dir=S3_OUT, region_name=REGION).cursor()
        cur.execute(
            f"SELECT region, events FROM {ATHENA_DB}.{ATHENA_TBL} "
            f"ORDER BY events DESC LIMIT {TOP_N}"
        )
        return list(cur.fetchall())
    except Exception as e:
        print(f"batch view unavailable: {e}")
        return []


def merge(batch, speed):
    counts = defaultdict(int)
    for region, events in batch:
        counts[region] = int(events)
    for region, delta in speed:
        counts[region] = counts.get(region, 0) + int(delta)
    return sorted(counts.items(), key=lambda x: -x[1])[:TOP_N]


def query():
    speed = get_speed_view()
    batch = get_batch_view()
    merged = merge(batch, speed)
    print(f"\nTop-{TOP_N} batch+speed:")
    for i, (region, count) in enumerate(merged, 1):
        print(f"  {i:2}. {region:<24} {count}")
    print(f"speed_only={speed}")
    print(f"batch_only={batch}")
    return merged


if __name__ == "__main__":
    query()
