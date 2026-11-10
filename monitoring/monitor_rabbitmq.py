#!/usr/bin/env python3
"""Read-only RabbitMQ queue and broker-host sampler (Python standard library).
Run on the broker host. Outputs queue CSV, sample CSV, and a summary JSON.
Rates are RabbitMQ management estimates, normally updated every five seconds.
No message bodies are fetched; no messages are consumed, purged, or acknowledged.
This does not measure timestamp-to-commit lag or resources on other hosts.
"""
import argparse
import base64
import csv
import getpass
import json
import os
from pathlib import Path
import re
import signal
import sys
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, build_opener, ProxyHandler, HTTPRedirectHandler


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def fetch(opener, url, token):
    req = Request(url, headers={"Authorization": "Basic " + token,
                                "Accept": "application/json"})
    with opener.open(req, timeout=10) as response:
        data = json.load(response)
    if not isinstance(data, list):
        raise ValueError("Expected a list of queues from API")
    return data


def queue_rows(queues):
    rows = []
    for q in queues:
        name = q["name"]
        stats = q.get("message_stats") or {}
        row = {"queue": name, "kind": "resort" if re.fullmatch(r"resort\.(?:[1-9]|10)", name) else "other",
               "ready": q["messages_ready"], "unacked": q["messages_unacknowledged"],
               "consumers": q.get("consumers", ""), "state": q.get("state", "")}
        row["depth"] = row["ready"] + row["unacked"]
        for key in ("publish", "deliver", "ack", "redeliver"):
            row[key + "_total"] = stats.get(key, "")
            row[key + "_rate"] = (stats.get(key + "_details") or {}).get("rate", "")
        rows.append(row)
    return rows


def sum_known(rows, key):
    values = [r[key] for r in rows]
    return sum(values) if values and all(isinstance(v, (int, float)) for v in values) else ""


class HostSampler:
    def __init__(self):
        self.previous = None
        self.interface = None
        try:
            for line in Path("/proc/net/route").read_text().splitlines()[1:]:
                fields = line.split()
                if fields[1] == "00000000":
                    self.interface = fields[0]
                    break
        except OSError:
            pass

    def sample(self):
        result = {"host_cpu_pct": "", "host_mem_available_mb": "", "network_interface": self.interface or "",
                  "host_rx_bytes_sec": "", "host_tx_bytes_sec": ""}
        try:
            cpu = list(map(int, Path("/proc/stat").read_text().splitlines()[0].split()[1:]))
            total, idle = sum(cpu[:8]), cpu[3] + cpu[4]
            mem = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
            result["host_mem_available_mb"] = int(mem["MemAvailable"].split()[0]) / 1024
            rx = tx = None
            for line in Path("/proc/net/dev").read_text().splitlines()[2:]:
                name, values = line.split(":", 1)
                if name.strip() == self.interface:
                    fields = values.split()
                    rx, tx = int(fields[0]), int(fields[8])
            now = time.monotonic()
            if self.previous:
                pt, pi, pr, ps, pn = self.previous
                if total > pt:
                    result["host_cpu_pct"] = round(100 * (1 - (idle - pi) / (total - pt)), 2)
                if rx is not None and pr is not None and now > pn:
                    result["host_rx_bytes_sec"] = (rx - pr) / (now - pn)
                    result["host_tx_bytes_sec"] = (tx - ps) / (now - pn)
            self.previous = total, idle, rx, tx, now
        except (OSError, ValueError, KeyError, IndexError):
            pass
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user", default="skitrak")
    parser.add_argument("--vhost", default="/")
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--duration", type=float, default=900)
    parser.add_argument("--out", default="monitoring-results")
    args = parser.parse_args()
    if args.interval < 1 or args.duration <= 0:
        parser.error("interval must be >=1 second and duration >0")
    password = getpass.getpass("RabbitMQ password (not saved): ")
    token = base64.b64encode((args.user + ":" + password).encode()).decode()
    del password
    opener = build_opener(ProxyHandler({}), NoRedirect())
    url = "http://127.0.0.1:15672/api/queues/" + quote(args.vhost, safe="")
    # Fail before creating outputs if authentication or endpoint access fails.
    try:
        queue_rows(fetch(opener, url, token))
    except HTTPError as exc:
        print("API HTTP %s: check username/password and permission to see all ten resort queues." % exc.code, file=sys.stderr)
        return 1
    except (URLError, ValueError, KeyError, TimeoutError, OSError):
        print("Cannot read queue statistics from local management API.", file=sys.stderr)
        return 1
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    out = Path(args.out) / ("rabbitmq-" + stamp)
    out.mkdir(parents=True, exist_ok=False)
    os.chmod(out, 0o700)
    stop = False
    def stopping(signum, frame):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGINT, stopping)
    signal.signal(signal.SIGTERM, stopping)
    queue_fields = ["utc", "elapsed_sec", "queue", "kind", "ready", "unacked", "depth", "consumers", "state"]
    for key in ("publish", "deliver", "ack", "redeliver"):
        queue_fields += [key + "_total", key + "_rate"]
    host = HostSampler()
    sample_fields = ["utc", "elapsed_sec", "status", "resort_queue_count", "resort_ready", "resort_unacked", "resort_depth",
                     "max_resort_depth", "resort_publish_rate", "resort_deliver_rate", "resort_ack_rate",
                     "dead_depth", "audit_depth", *host.sample().keys()]
    depths, per_queue_peak, errors = [], {}, 0
    start = time.monotonic()
    print("Recording to " + str(out.resolve()), flush=True)
    try:
        with (out / "queues.csv").open("w", newline="") as qf, (out / "samples.csv").open("w", newline="") as sf:
            qw, sw = csv.DictWriter(qf, queue_fields), csv.DictWriter(sf, sample_fields)
            qw.writeheader(); sw.writeheader()
            while not stop and time.monotonic() - start < args.duration:
                tick = time.monotonic()
                elapsed = round(tick - start, 3)
                utc = datetime.now(timezone.utc).isoformat()
                sample = {"utc": utc, "elapsed_sec": elapsed, **host.sample()}
                try:
                    rows = queue_rows(fetch(opener, url, token))
                    resorts = [r for r in rows if r["kind"] == "resort"]
                    for row in rows:
                        qw.writerow({"utc": utc, "elapsed_sec": elapsed, **row})
                    sample["resort_queue_count"] = len(resorts)
                    if len(resorts) != 10:
                        raise ValueError("Not all ten resort queues visible")
                    depth = sum(r["depth"] for r in resorts)
                    depths.append(depth)
                    for row in resorts:
                        per_queue_peak[row["queue"]] = max(per_queue_peak.get(row["queue"], 0), row["depth"])
                    sample.update(status="ok", resort_ready=sum(r["ready"] for r in resorts),
                                  resort_unacked=sum(r["unacked"] for r in resorts), resort_depth=depth,
                                  max_resort_depth=max(r["depth"] for r in resorts))
                    for key in ("publish", "deliver", "ack"):
                        sample["resort_" + key + "_rate"] = sum_known(resorts, key + "_rate")
                    for name, field in (("skitrak.dead", "dead_depth"), ("leaderboard.audit", "audit_depth")):
                        sample[field] = next((r["depth"] for r in rows if r["queue"] == name), "")
                    print("%7.1fs depth=%s max-resort=%s publish/s=%s ack/s=%s" %
                          (elapsed, depth, sample["max_resort_depth"], sample["resort_publish_rate"], sample["resort_ack_rate"]), flush=True)
                except HTTPError as exc:
                    sample["status"] = "HTTP_" + str(exc.code); errors += 1
                    if exc.code in (401, 403):
                        stop = True
                except (URLError, ValueError, KeyError, TypeError, TimeoutError, OSError):
                    sample["status"] = "sample_error"; errors += 1
                sw.writerow(sample); qf.flush(); sf.flush()
                while not stop and time.monotonic() - tick < args.interval:
                    time.sleep(min(0.2, max(0, args.interval - (time.monotonic() - tick))))
    finally:
        summary = {"scope": "entire recording; filter CSV timestamps for benchmark-only metrics",
                   "successful_samples": len(depths), "failed_samples": errors,
                   "sampled_peak_total_resort_depth": max(depths) if depths else None,
                   "sample_mean_total_resort_depth": sum(depths) / len(depths) if depths else None,
                   "sampled_peak_by_resort": per_queue_peak,
                   "note": "Sampled observations, not a guarantee of continuous depth. Rates are management estimates. "
                           "Audit queue excluded from resort backlog. Host metrics describe the broker host only. "
                           "Consumer lag, TTL expiry, and no-loss reconciliation require separate evidence."}
        (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print("Saved: " + str(out.resolve()), flush=True)
    return 0 if depths and not errors else 2


if __name__ == "__main__":
    sys.exit(main())
