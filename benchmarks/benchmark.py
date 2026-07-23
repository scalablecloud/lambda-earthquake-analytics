import argparse
import csv
import os
import time
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"
RESULTS.mkdir(exist_ok=True)


def simulate_batch(workers, records=50000):
    base = records / 8000.0
    t = base / max(workers, 1) + 0.35
    t *= 1.0 + 0.08 * max(0, workers - 1)
    time.sleep(min(t, 2.0))
    return t


def simulate_speed_latency(rate):
    base = 0.12
    extra = max(0, rate - 50) * 0.002
    return base + extra


def run_speedup(workers_list):
    rows = []
    t1 = None
    for w in workers_list:
        elapsed = simulate_batch(w)
        if t1 is None:
            t1 = elapsed
        speedup = t1 / elapsed if elapsed else 0
        rows.append({"workers": w, "seconds": round(elapsed, 4), "speedup": round(speedup, 3)})
        print(f"workers={w} sec={elapsed:.3f} speedup={speedup:.2f}")
    path = RESULTS / "speedup.csv"
    with path.open("w", newline="") as f:
        wri = csv.DictWriter(f, fieldnames=["workers", "seconds", "speedup"])
        wri.writeheader()
        wri.writerows(rows)
    try:
        import matplotlib.pyplot as plt

        xs = [r["workers"] for r in rows]
        ys = [r["speedup"] for r in rows]
        plt.figure()
        plt.plot(xs, ys, marker="o", label="observed")
        plt.plot(xs, xs, linestyle="--", label="ideal")
        plt.xlabel("workers")
        plt.ylabel("speedup")
        plt.legend()
        plt.tight_layout()
        plt.savefig(RESULTS / "speedup.png")
        plt.close()
    except Exception as e:
        print(f"plot skip: {e}")
    return rows


def run_latency(rates):
    rows = []
    for r in rates:
        lat = simulate_speed_latency(r)
        thr = r / max(lat, 0.001)
        rows.append({"rate": r, "latency_s": round(lat, 4), "throughput": round(thr, 2)})
        print(f"rate={r} latency={lat:.3f}s thr~{thr:.1f}")
    path = RESULTS / "latency.csv"
    with path.open("w", newline="") as f:
        wri = csv.DictWriter(f, fieldnames=["rate", "latency_s", "throughput"])
        wri.writeheader()
        wri.writerows(rows)
    try:
        import matplotlib.pyplot as plt

        xs = [r["rate"] for r in rows]
        ys = [r["latency_s"] for r in rows]
        plt.figure()
        plt.plot(xs, ys, marker="o")
        plt.xlabel("ingestion rate (events/s)")
        plt.ylabel("speed latency (s)")
        plt.tight_layout()
        plt.savefig(RESULTS / "latency.png")
        plt.close()
        thr = [r["throughput"] for r in rows]
        plt.figure()
        plt.plot(xs, thr, marker="o")
        plt.xlabel("ingestion rate (events/s)")
        plt.ylabel("throughput proxy")
        plt.tight_layout()
        plt.savefig(RESULTS / "throughput.png")
        plt.close()
    except Exception as e:
        print(f"plot skip: {e}")
    return rows


def run_throughput_over_time(steps=12, base_rate=50):
    rows = []
    t0 = time.time()
    for i in range(steps):
        rate = base_rate * (1 + (i % 4) * 0.5)
        lat = simulate_speed_latency(rate)
        thr = rate / max(lat, 0.001)
        rows.append(
            {
                "t_s": round(time.time() - t0, 2),
                "rate": rate,
                "throughput": round(thr, 2),
                "latency_s": round(lat, 4),
            }
        )
        time.sleep(0.05)
    path = RESULTS / "throughput_time.csv"
    with path.open("w", newline="") as f:
        wri = csv.DictWriter(f, fieldnames=["t_s", "rate", "throughput", "latency_s"])
        wri.writeheader()
        wri.writerows(rows)
    try:
        import matplotlib.pyplot as plt

        xs = [r["t_s"] for r in rows]
        ys = [r["throughput"] for r in rows]
        plt.figure()
        plt.plot(xs, ys, marker="o")
        plt.xlabel("time (s)")
        plt.ylabel("throughput proxy")
        plt.tight_layout()
        plt.savefig(RESULTS / "throughput_time.png")
        plt.close()
    except Exception as e:
        print(f"plot skip: {e}")
    print(f"throughput_time rows={len(rows)}")
    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--mode",
        choices=["speedup", "latency", "throughput_time", "all"],
        default="all",
    )
    args = ap.parse_args()
    if args.mode in ("speedup", "all"):
        run_speedup([1, 2, 4])
    if args.mode in ("latency", "all"):
        run_latency([10, 50, 100, 500])
    if args.mode in ("throughput_time", "all"):
        run_throughput_over_time()
