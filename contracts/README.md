# Event contracts

Kafka topics emitted by the Campus Delivery backend. Topic names carry a major version suffix and
every message carries `schemaVersion`. Rules:

- **Additive changes** (new optional fields, new event types) bump `schemaVersion` and keep the topic.
- **Breaking changes** (renamed/removed fields, changed semantics) create a new topic (`*.v2`); the
  producer dual-writes during migration.
- Consumers must ignore unknown fields and unknown event types.

| Topic                  | Key         | Schema                             | Delivery                                   |
|------------------------|-------------|------------------------------------|--------------------------------------------|
| `order-events.v1`      | `orderId`   | `order-event.v1.schema.json`       | transactional outbox, at-least-once, ordered per key |
| `courier-events.v1`    | `courierId` | `courier-event.v1.schema.json`     | transactional outbox, at-least-once, ordered per key |
| `courier-locations.v1` | `courierId` | `courier-location.v1.schema.json`  | fire-and-forget telemetry, may be lost or duplicated |

Every outbox message also has Kafka headers `event-type` and `schema-version`. Consumers should
deduplicate on `eventId` (a monotonically increasing `bigint` from the `events` table).
