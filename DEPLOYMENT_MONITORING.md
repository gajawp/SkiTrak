# SkiTrak deployment and monitoring runbook

This documents the existing AWS lab deployment as observed on 10 November 2026 UTC. It is not a bootstrap installer. Public IP addresses can change when instances stop and start; verify them before reuse.

## Components and addresses

| Component | Public IPv4 at measurement | Private IPv4 |
| --- | --- | --- |
| Ingest 1 | 35.85.34.48 | 172.31.20.13 |
| Ingest 2 | 18.237.104.19 | 172.31.23.45 |
| Ingest 3 | 16.147.50.172 | 172.31.19.24 |
| Ingest 4 | 16.147.39.85 | 172.31.23.238 |
| Consumer | 35.91.229.209 | 172.31.20.159 |
| RabbitMQ | 34.214.200.174 | 172.31.23.167 |

ALB base URL: http://skitrak-alb-1256428710.us-west-2.elb.amazonaws.com/ski-servlet

Four Tomcat ingest hosts are t3.micro; consumer and broker are t3.small. Tomcat is under `/opt/tomcat9`; the consumer JAR is `/home/ec2-user/skitrak-consumer/consumer.jar`. Persistent consumer databases are under `/home/ec2-user/skitrak-consumer/data`. Do not delete them between recovery checks. The RabbitMQ container is `skitrak-rabbitmq`.

## Load-balancer settings

| Setting | Observed value |
| --- | --- |
| Target group | skitrak-ingest-tg |
| Backend protocol / port | HTTP / 8080 |
| Health path | /ski-servlet/health |
| Health interval / timeout | 30 / 5 seconds |
| Healthy / unhealthy threshold | 2 / 3 |
| Health success code | 200 |
| Routing | Round robin |
| Cookie stickiness | Load-balancer generated; 300 seconds |
| ALB connection idle timeout | 300 seconds |
| HTTP client keepalive duration | 3,600 seconds |
| Deregistration delay | 300 seconds |

Keepalive duration, stickiness duration and idle timeout are different settings. An established WebSocket stays with its accepting target; after target failure the client must reconnect. An immediate request can fail before health checks remove a failed target.

## Configuration and ownership

Each Tomcat `bin/setenv.sh` supplies a unique `SERVER_ID=ingest-N`. AMQP clients use the private broker address on port 5672. Supply `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD` and `RABBITMQ_VHOST` through local environment configuration; do not commit credentials or PEM keys.

The current consumer systemd drop-in `/etc/systemd/system/skitrak-consumer.service.d/100-processing-pool.conf` sets:

```ini
[Service]
Environment=CONSUMER_THREADS=2
Environment=CONSUMER_MIN_THREADS=2
Environment=CONSUMER_MAX_THREADS=10
Environment=CONSUMER_DYNAMIC_SCALING=true
```

Batch size is 200 and batch wait is 20 ms. `CONSUMER_DATA_DIR`, `CONSUMER_HTTP_HOST`, `CONSUMER_HTTP_PORT`, `CONSUMER_DB_CACHE_KB` and `TOP_N` are also supported. Inspect the installed unit and drop-ins when reproducing a run; environment-file overrides matter. Do not expose diagnostic ports publicly just to run the verifier.

The consumer has `Restart=on-failure` with a five-second delay. RabbitMQ Docker policy is `unless-stopped`; a deliberate `docker stop` requires an explicit start. Tomcat was manually started after VM restart in the recorded experiments; automatic Tomcat startup was not established.

## Health and diagnostics

On a consumer SSH session:

```bash
systemctl is-active skitrak-consumer
curl -fsS --max-time 30 http://localhost:9090/health
curl -fsS --max-time 30 http://localhost:9090/stats
```

On each ingest SSH session:

```bash
curl -fsS --max-time 15 http://localhost:8080/ski-servlet/health
```

On the broker SSH session:

```bash
sudo docker ps --filter name=skitrak-rabbitmq
sudo docker exec skitrak-rabbitmq rabbitmqctl list_queues \
  name messages_ready messages_unacknowledged consumers
```

Distinguish resort queues from `leaderboard.audit`: the audit queue intentionally has no consumer and retained snapshots. Inspect `skitrak.dead` separately. Empty executor `queuedBatches` is not proof that RabbitMQ resort queues are empty.

On the Mac, keep this forwarding session open:

```bash
ssh -i "$HOME/Desktop/Courses/DistributedSystems/cs6650-lab1-key.pem" \
  -N -T -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -L 127.0.0.1:19090:172.31.20.159:9090 ec2-user@35.85.34.48
```

## Reproduce validation

From `client-part2`, with the tunnel open:

```bash
.venv-verify/bin/python src/main/java/verify_leaderboards.py \
  --base 'http://skitrak-alb-1256428710.us-west-2.elb.amazonaws.com/ski-servlet' \
  --consumer 'http://127.0.0.1:19090'
```

For isolated POST-to-WebSocket timing, stop other producers:

```bash
.venv-verify/bin/python src/main/java/measure_websocket_latency.py \
  --base 'http://skitrak-alb-1256428710.us-west-2.elb.amazonaws.com/ski-servlet' \
  --consumer 'http://127.0.0.1:19090' --resort 2 --samples 20
```

This sends 20 real swipes and validates one unique-event increment per trial. Do not run it simultaneously with a load client. It measures isolated end-to-end time, not WebSocket-only or concurrent-load latency.

## Resource collection and workload

Start host collectors before the load. From the project root on the Mac:

```bash
metrics_run="system-metrics-$(date -u +%Y%m%d-%H%M%S)"
mkdir -p "results/$metrics_run"
for ip in 35.85.34.48 18.237.104.19 16.147.50.172 \
          16.147.39.85 35.91.229.209 34.214.200.174; do
  ssh -i "$HOME/Desktop/Courses/DistributedSystems/cs6650-lab1-key.pem" \
    ec2-user@"$ip" \
    'export LC_ALL=C; date -u; hostname; sar -u -r -n DEV 2 300' \
    > "results/$metrics_run/$ip.log" 2>&1 &
done
printf 'Recording in results/%s\n' "$metrics_run"
```

Keep that terminal open. In another terminal, from `client-part2`, save timing boundaries and the load output:

```bash
load_log="../results/assignment2-monitored-500k-$(date -u +%Y%m%d-%H%M%S).log"
(
  date -u
  java -Dskitrak.totalSwipes=500000 \
    -Dskitrak.warmupSwipes=32000 \
    -Dskitrak.drainTimeoutSeconds=600 \
    -cp target/classes LoadClient \
    'http://skitrak-alb-1256428710.us-west-2.elb.amazonaws.com/ski-servlet' \
    512 10 2026 1
  client_status=$?
  date -u
  exit "$client_status"
) 2>&1 | tee "$load_log"
```

Inspect the saved output for `RUN COMPLETE` and the expected unique-count delta; `tee` success alone is insufficient. Save `/stats` immediately before and after if comparing process-local metrics. Restarting the consumer resets commit metrics; it must not reset the persisted event count.

After collection finishes, run `wait` in the original collector shell and inspect all six logs for complete timestamps and errors. Do not assume file presence means the requested sample count was recorded: the uploaded experiment contained 202 two-second samples per host, not the requested 300. It nevertheless covered the workload and drain.

The existing `monitor_rabbitmq.py` on the broker records queue depth and rates through the localhost management API. Run it concurrently when queue evidence is needed:

```bash
python3 /home/ec2-user/monitor_rabbitmq.py \
  --user skitrak --interval 5 --duration 900 \
  --out /home/ec2-user/monitoring-results
```

It prompts for the password. Retain the script in the repository's `monitoring/` directory as well as its CSV outputs; this document does not replace the script.

## Evidence and interpretation

The latest 500k run increased the count from 9,310,076 to 9,810,076, with zero HTTP failures/retries. Final verification `leaderboard-verification-20261109-164800.json` passed all ten resorts with a stable count. Keep the raw load log, `part2-512threads-1794271327161` CSVs, six host logs, queue-monitor CSVs and isolated latency JSON with the report.

A request UUID must remain unchanged across retries after an uncertain outcome. HTTP 201 verifies confirmed publication, not end-to-end completion. Event counts establish unique-event accounting; current snapshot comparisons establish current state. Neither proves every historical WebSocket update was delivered. Publish/ACK rates sampled at intervals are not interchangeable with exact unique-event deltas.

## Before submission

- Put the architecture/results PDFs in a repository documentation directory and link them from the main README.
- Put this runbook under `deployment/` and the actual monitoring/verifier scripts under their documented paths.
- Review `AI_USAGE.md` for accurate personal attribution and retain only genuine transcript material.
- Include the Git repository URL in the Canvas submission PDF. It has not yet been supplied for the generated documents.
- Verify `server-v2`, `consumer`, `deployment` and `monitoring` are present; run the Maven tests and inspect Git changes before committing.
- Ensure no key files, credentials, databases, build outputs or unintended large CSVs are staged.
- Preserve raw evidence outside Git if file sizes are impractical, and document its location rather than silently omitting it.
- State explicitly that heavy-load queue-depth and commit-lag targets were not met. Do not label the deployment highly available.
