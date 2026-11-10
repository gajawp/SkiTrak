# AI usage reflection - SkiTrak Assignment 2

Author: Preethi Gajawada

## Scope of assistance

I used ChatGPT for design discussion, code drafts, debugging and experiment interpretation. Specific areas included RabbitMQ publisher confirms and channel ownership, per-resort consumer batching, H2 aggregation and persistent event deduplication, a durable outbox, the leaderboard fanout backplane, dynamic processing-pool sizing, ALB configuration checks, recovery-test commands, monitoring, snapshot verification and latency probes. AI also helped draft the architecture and results documents.

The deployed commands and experiments were run from my Mac against my AWS lab instances. Maven reported 22 passing consumer tests after the metrics changes. I provided terminal output and monitoring files for analysis. AI-generated code and explanations required validation; a successful build alone did not establish distributed correctness or performance.

## Suggestions accepted

### 1. Persist event deduplication with aggregation

I used an event UUID and a fingerprint of the swipe fields, storing the processed ID in the same H2 transaction as totals, changed snapshots and the outbox. This matters when a client retries after an uncertain publish or the broker redelivers after a consumer crash. An in-memory set would lose its history on restart.

Validation: submitting one UUID twice increased the unique count once. After a SIGKILL and automatic consumer restart, submitting the same UUID again left the count unchanged. This supports persistent idempotent aggregation, not exactly-once message delivery.

### 2. Use a fanout backplane for connection locality

The consumer publishes snapshots to leaderboard.fanout, and each ingest server receives them through its own queue and pushes to local WebSocket sessions. This supports a subscriber connected to a different ingest server from the one that accepted the swipe. Sticky sessions alone do not deliver consumer updates to the correct server.

Validation included real swipe-driven leaderboard changes, subscriber reconnection observations, and final comparison of WebSocket and consumer snapshots for all ten resorts.

## Suggestions rejected, corrected or limited after testing

### 1. Full JSON equality in the first latency probe

The first AI-generated probe compared the entire initial WebSocket JSON with the consumer JSON. It failed before sending any test swipe even though the existing verifier passed all ten leaderboards. The old failure report did not save the two snapshots, so the exact differing field was not established.

The corrected probe compares resort, day, version, ranks, skier IDs and totals, and saves the initial snapshots for diagnostics. It retains exact next-version and consumer-count checks. Twenty subsequent isolated probes passed, averaging 51.20 ms from POST dispatch to matching WebSocket receipt. I do not interpret this as concurrent-load or WebSocket-only latency.

### 2. HTTP smoke-test commands needed corrections

An early suggested POST omitted the servlet mapping /skiers and returned 404. A corrected route still returned 400 because the suggested X-Event-ID had an alb-check- prefix instead of being a plain UUID. Source inspection showed @WebServlet("/skiers/*") and strict UUID validation. The working request used /ski-servlet/skiers/2/seasons/2026/days/1/skiers/85585 and an unmodified uuidgen value.

### 3. More configured threads did not imply more parallel aggregation

I tested fixed capacities of 10, 20, 40 and 80 plus dynamic 2-10. Each resort coordinator submits only one batch at a time and there are ten resorts, so the implementation cannot execute 80 independent resort tasks concurrently. I therefore report configured capacity separately from actual active work. The observed runs did not show a completion-throughput improvement above ten; database growth and changing host conditions also limit causal comparisons.

## A decision that required reasoning beyond AI output

I had to distinguish acceptance, processing and live delivery. HTTP 201 means publisher confirmation, not completion of aggregation or delivery to every subscriber. A consumer count reaching the expected delta verifies unique-event accounting but does not prove every WebSocket update arrived. Snapshot equality establishes current state, while a live isolated probe measures a different property.

This distinction mattered in the in-flight consumer crash test: all 100,000 HTTP requests succeeded, but completion polling failed when the consumer connection closed. I retained that failure rather than labeling the client run clean. Later consumer counts and leaderboard verification reconciled the workload separately.

## Limitations retained in the submission

- Heavy-load queue depth exceeded 1,000 messages and event-to-commit lag exceeded 100 ms. Those performance targets were not met.
- Final process-local commit metrics combine the 100k load run, 20 isolated probes and the latest 500k run; they are not a standalone measurement of the final run.
- Consumer and broker are single-host services. These tests do not establish high availability or survival of disk loss.
- Broker recovery evidence is from a graceful container stop/start, not a crash with pending messages.
- Host resource monitoring covers a later run and cannot be retroactively assigned to earlier experiments.

## Transcript evidence

My AI Chat link - https://chatgpt.com/share/6af28966-74e8-83e8-a59c-6f90455f6e67

This reflection refers to the actual project conversation, terminal outputs, source corrections and saved results. No transcript link has been inserted because no share link was supplied. If a transcript is included, export the real exchange and remove credentials before publishing; do not fabricate a transcript.
