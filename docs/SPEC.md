# Campus Delivery — technical spec (as given)

**Goal.** Order → courier accept → live tracking → dispatcher visibility, correct under concurrent access and flaky networks.

**Stack.** React + TypeScript (Vite) for the customer and dispatcher web apps; Java 21 + Spring Boot (REST + WebSocket); PostgreSQL + PostGIS; Kafka for order and location events (feeds the data project below); Docker Compose, later deployed through the Reliability Lab pipeline. Simulated couriers first; SwiftUI courier app with MapKit and Core Location only after the web flow is solid.

**Core flows**
- Customer: place order, watch status and courier position
- Courier: go online, receive offer, accept, send location, mark picked up / delivered
- Dispatcher: live map of orders and couriers, find stuck orders, reassign or cancel

**State machine.** `CREATED → OFFERED → ACCEPTED → PICKED_UP → DELIVERED`, plus `CANCELLED`; illegal transitions rejected at the database layer.

**Hard properties (the interview material)**
- Exactly one courier per order: row lock or optimistic version; an integration test fires 20 concurrent accepts and exactly one wins
- Idempotent order creation: client idempotency key; retried requests return the same order
- Location updates: batched, out-of-order handled by timestamp, last-known position with a staleness indicator
- Reconnect: WebSocket resumes from last event ID and replays missed updates
- Assignment: nearest online courier via PostGIS `ST_DWithin`; offer times out and re-offers
- Auth: JWT with customer / courier / dispatcher roles

**Deliverables.** `docker compose up` with seeded merchants and simulated couriers; integration tests for the four properties above; README with architecture diagram; short demo GIF.

**Out of scope.** Payments, routing engine, push notifications. Budget: 3–4 weeks.

## Data engineering project — Delivery Analytics Pipeline

Built on Campus Delivery's Kafka events.

- Producers: order state changes and courier pings to Kafka topics with versioned schemas
- Streaming: Spark Structured Streaming computes per-minute active orders, time-to-accept, and courier utilization; watermarks for late data; checkpointed exactly-once sink
- Batch: daily Spark job writes Parquet to MinIO (S3-compatible), partitioned by date; delivery-time distributions and courier performance
- Orchestration: Airflow DAG with retries and backfill
- Data quality: checks for negative durations and orphan order IDs; bad records quarantined, not dropped
- Replay: reprocess from a Kafka offset and prove the sinks are idempotent
- Serving: metrics table read by the dispatcher dashboard

Measure throughput, end-to-end latency, and replay correctness. Skip HDFS; MinIO plays that role. Budget: 2 weeks, once the app emits events.
