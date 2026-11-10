#!/usr/bin/env python3
"""Isolated POST dispatch -> matching leaderboard update; no cross-host clock subtraction."""
import argparse
import asyncio
import json
import math
from pathlib import Path
import statistics
import time
from datetime import datetime, timezone
import urllib.request
import uuid

import websockets


def http_json(url, payload=None, event_id=None):
    headers = {}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode()
        headers = {"Content-Type": "application/json", "X-Event-ID": event_id}
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.status, json.load(response)


def skier_total(snapshot, skier):
    return next((int(row["vertical"]) for row in snapshot.get("top", [])
                 if str(row["skierID"]) == skier), None)


def canonical(snapshot, resort, day):
    """Compare leaderboard state, independently of transport metadata."""
    if str(snapshot["resortID"]) != str(resort) or str(snapshot["dayID"]) != str(day):
        raise RuntimeError("Wrong resort/day in snapshot")
    version = snapshot["version"]
    if type(version) is not int or version < 0:
        raise RuntimeError("Invalid snapshot version")
    if not isinstance(snapshot["top"], list):
        raise RuntimeError("Invalid top list")
    entries = []
    for index, row in enumerate(snapshot["top"], 1):
        if type(row["rank"]) is not int or row["rank"] != index:
            raise RuntimeError("Invalid rank sequence")
        if type(row["vertical"]) is not int:
            raise RuntimeError("Invalid vertical total")
        entries.append((row["rank"], str(row["skierID"]), row["vertical"]))
    return str(resort), str(day), version, entries


async def receive_match(socket, resort, day, version, skier, previous_total, start_ns):
    while True:
        raw = await socket.recv()
        received_ns = time.perf_counter_ns()
        snapshot = json.loads(raw)
        if str(snapshot.get("resortID")) != str(resort) or str(snapshot.get("dayID")) != str(day):
            raise RuntimeError("Unexpected resort/day on subscriber")
        actual_version = int(snapshot.get("version", -1))
        if actual_version < version:
            continue
        if actual_version != version:
            raise RuntimeError("Leaderboard version skipped; isolated correlation cannot be established")
        total = skier_total(snapshot, skier)
        if total is None or total <= previous_total:
            raise RuntimeError("Expected skier total did not increase")
        return snapshot, (received_ns - start_ns) / 1_000_000.0


async def measure(args, report):
    base = args.base.rstrip("/")
    consumer = args.consumer.rstrip("/")
    _, before = await asyncio.to_thread(http_json, consumer + "/stats")
    initial_count = int(before["uniqueSwipesProcessed"])
    report["initialConsumerCount"] = initial_count
    _, baseline = await asyncio.to_thread(http_json, f"{consumer}/leaderboard/{args.resort}/{args.day}")
    if not baseline.get("top"):
        raise RuntimeError("Need an existing nonempty leaderboard")
    skier = str(baseline["top"][0]["skierID"])
    report["skierID"] = skier
    ws_base = ("wss://" + base[8:]) if base.startswith("https://") else ("ws://" + base[7:])
    endpoint = f"{ws_base}/leaderboard/{args.resort}?dayID={args.day}"
    async with websockets.connect(endpoint, open_timeout=15, ping_interval=20, ping_timeout=20) as socket:
        initial = json.loads(await asyncio.wait_for(socket.recv(), args.timeout))
        report["initialConsumerSnapshot"] = baseline
        report["initialWebSocketSnapshot"] = initial
        if canonical(initial, args.resort, args.day) != canonical(baseline, args.resort, args.day):
            raise RuntimeError("WebSocket initial leaderboard state differs from consumer; see saved snapshots")
        _, confirmed = await asyncio.to_thread(http_json, f"{consumer}/leaderboard/{args.resort}/{args.day}")
        _, confirmed_stats = await asyncio.to_thread(http_json, consumer + "/stats")
        if (canonical(confirmed, args.resort, args.day) != canonical(baseline, args.resort, args.day)
                or int(confirmed_stats["uniqueSwipesProcessed"]) != initial_count):
            raise RuntimeError("Consumer changed during initial checks; pause other producers")
        print(f"CONNECTED resort={args.resort} day={args.day} version={baseline['version']} skier={skier}", flush=True)
        for index in range(1, args.samples + 1):
            previous_total = skier_total(baseline, skier)
            event_id = str(uuid.uuid4())
            version = int(baseline["version"]) + 1
            url = f"{base}/skiers/{args.resort}/seasons/2026/days/{args.day}/skiers/{skier}"
            start_ns = time.perf_counter_ns()
            receive = asyncio.create_task(receive_match(socket, args.resort, args.day, version,
                                                        skier, previous_total, start_ns))
            try:
                status, _ = await asyncio.to_thread(http_json, url, {"time": 1, "liftID": 1}, event_id)
                http_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
                if status != 201:
                    raise RuntimeError(f"Expected HTTP 201, received {status}")
                snapshot, elapsed_ms = await asyncio.wait_for(receive, args.timeout)
                canonical(snapshot, args.resort, args.day)
            finally:
                if not receive.done():
                    receive.cancel()
                await asyncio.gather(receive, return_exceptions=True)
            _, stats = await asyncio.to_thread(http_json, consumer + "/stats")
            actual_count = int(stats["uniqueSwipesProcessed"])
            if actual_count != initial_count + index:
                raise RuntimeError("Consumer count differs from one unique event per sample; check concurrent traffic")
            report["samples"].append({"sample": index, "eventID": event_id, "httpStatus": status,
                                      "versionBefore": version - 1, "versionAfter": version,
                                      "verticalBefore": previous_total, "verticalAfter": skier_total(snapshot, skier),
                                      "postDispatchToWebSocketMs": elapsed_ms, "httpResponseObservedMs": http_ms})
            baseline = snapshot
            report["finalConsumerCount"] = actual_count
            print(f"sample={index} version={version} post_to_ws_ms={elapsed_ms:.3f} http_ms={http_ms:.3f}", flush=True)
    values = sorted(sample["postDispatchToWebSocketMs"] for sample in report["samples"])
    report["summaryMs"] = {"count": len(values), "mean": statistics.mean(values),
                           "median": statistics.median(values), "p95NearestRank": values[math.ceil(.95 * len(values)) - 1],
                           "min": min(values), "max": max(values)}
    report["status"] = "PASS"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--consumer", default="http://127.0.0.1:19090")
    parser.add_argument("--resort", type=int, default=2)
    parser.add_argument("--day", type=int, default=1)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--out", default="../results")
    args = parser.parse_args()
    if not args.base.startswith(("http://", "https://")) or not 1 <= args.resort <= 10 or args.day < 1 or not 1 <= args.samples <= 1000 or args.timeout <= 0:
        parser.error("Invalid base URL, resort, day, sample count, or timeout")
    report = {"status": "FAIL", "utc": datetime.now(timezone.utc).isoformat(),
              "definition": "Mac monotonic clock: immediately before dispatching POST to matching WebSocket receive; includes local scheduling, ingestion, queueing, commit and delivery",
              "correlation": "one producer, one event at a time; exact next leaderboard version, increased selected skier total and exact consumer count delta; payload does not carry eventID",
              "scope": "isolated sequential probes, not concurrent-load latency or WebSocket-only network latency",
              "resort": args.resort, "day": args.day, "requestedSamples": args.samples, "samples": []}
    try:
        asyncio.run(measure(args, report))
    except (Exception, KeyboardInterrupt) as error:
        report["error"] = f"{type(error).__name__}: {error}"
        print(report["error"], flush=True)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / ("websocket-latency-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f") + ".json")
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report.get("summaryMs", {})), flush=True)
    print(f"{report['status']}: {path}", flush=True)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
