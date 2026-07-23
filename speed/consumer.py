import json
import os
import time
from collections import defaultdict, deque


class SlidingWindow:
    def __init__(self, window_s=300, bucket_s=10):
        self.window_s = window_s
        self.bucket_s = bucket_s
        self.buckets = deque()

    def add(self, item, mag=None):
        now = time.time()
        if not self.buckets or now - self.buckets[-1][0] >= self.bucket_s:
            self.buckets.append((now, defaultdict(int), defaultdict(float), defaultdict(int)))
        b = self.buckets[-1]
        b[1][item] += 1
        if mag is not None:
            b[2][item] += float(mag)
            b[3][item] += 1
        self._evict(now)

    def _evict(self, now):
        cutoff = now - self.window_s
        while self.buckets and self.buckets[0][0] < cutoff:
            self.buckets.popleft()

    def top(self, n):
        counts = defaultdict(int)
        mag_sum = defaultdict(float)
        mag_n = defaultdict(int)
        for _, c, ms, mn in self.buckets:
            for k, v in c.items():
                counts[k] += v
            for k, v in ms.items():
                mag_sum[k] += v
            for k, v in mn.items():
                mag_n[k] += v
        ranked = sorted(counts.items(), key=lambda x: -x[1])[:n]
        out = []
        for region, cnt in ranked:
            avg = mag_sum[region] / mag_n[region] if mag_n[region] else 0.0
            out.append((region, cnt, avg))
        return out


def _aws():
    import boto3
    from dotenv import load_dotenv

    load_dotenv()
    region = os.getenv("AWS_REGION", "us-east-1")
    stream = os.getenv("KINESIS_STREAM", "earthquake-stream")
    dynamo_name = os.getenv("DYNAMO_TABLE", "earthquake-speed-view")
    window_s = int(os.getenv("WINDOW_SECONDS", "300"))
    top_n = int(os.getenv("TOP_N", "10"))
    kinesis = boto3.client("kinesis", region_name=region)
    table = boto3.resource("dynamodb", region_name=region).Table(dynamo_name)
    return kinesis, table, stream, window_s, top_n


def process(window, record):
    region = record.get("region") or "unknown"
    mag = record.get("mag")
    window.add(region, mag)


def hotspot_flags(top):
    if not top:
        return {}
    counts = [c for _, c, _ in top]
    mean = sum(counts) / len(counts)
    thr = max(mean * 2.0, mean + 1.0) if mean > 0 else 1.0
    return {region: count >= thr for region, count, _ in top}


def flush(window, table, top_n, window_s):
    top = window.top(top_n)
    flags = hotspot_flags(top)
    ts = str(int(time.time()))
    for rank, (region, count, avg_mag) in enumerate(top, 1):
        table.put_item(
            Item={
                "pk": f"speed#{rank}",
                "region": region,
                "count": count,
                "avg_mag": str(round(avg_mag, 3)),
                "hotspot": "1" if flags.get(region) else "0",
                "ts": ts,
                "window": window_s,
            }
        )
    print(f"[{ts}] top={top[:3]} hotspots={[r for r, f in flags.items() if f]}")


def shard_iterators(kinesis, stream):
    desc = kinesis.describe_stream(StreamName=stream)["StreamDescription"]
    its = []
    for s in desc["Shards"]:
        it = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=s["ShardId"],
            ShardIteratorType="LATEST",
        )["ShardIterator"]
        its.append(it)
    return its


def run():
    kinesis, table, stream, window_s, top_n = _aws()
    window = SlidingWindow(window_s=window_s, bucket_s=10)
    its = shard_iterators(kinesis, stream)
    last_flush = time.time()
    flush_s = 15
    while True:
        new_its = []
        for it in its:
            if not it:
                continue
            resp = kinesis.get_records(ShardIterator=it, Limit=100)
            for rec in resp.get("Records", []):
                try:
                    body = json.loads(rec["Data"])
                    process(window, body)
                except Exception as e:
                    print(f"skip record: {e}")
            new_its.append(resp.get("NextShardIterator"))
        its = new_its or shard_iterators(kinesis, stream)
        if time.time() - last_flush >= flush_s:
            flush(window, table, top_n, window_s)
            last_flush = time.time()
        time.sleep(1)


if __name__ == "__main__":
    run()
