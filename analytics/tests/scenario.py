"""A small, fully known delivery day used by both the batch and the streaming tests.

Timeline (UTC, 2026-09-28):
  A  created 10:00:10, offered 10:00:11, accepted 10:00:30 (+dup), picked 10:02:00, delivered 10:05:30  courier k1
  B  created 10:01:05, accepted 10:01:50, picked 10:03:00, cancelled 10:04:10                              courier k2
  C  created 10:02:20 and never finished (still open)
  D  created 10:03:00, accepted 10:03:20, picked 10:04:00, delivered 10:06:00 -- but the DELIVERED
     payload carries a corrupted acceptedAt (10:02:30) => negative duration => quarantined
  plus one unparseable value, one unknown event type, one schemaVersion=2 envelope.
Pings: k1 at 10:00:05, :20 (idle), :35, :50 (on A); 10:01:10, :30, :50 (on A). One ping with lat 95.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

BASE = datetime(2026, 9, 28, 10, 0, 0, tzinfo=timezone.utc)
MERCHANT = {"id": "m-1", "name": "Library Café", "location": {"lat": 37.873, "lng": -122.26}}


def t(mm: int, ss: int = 0) -> datetime:
    return BASE + timedelta(minutes=mm, seconds=ss)


def iso(dt: datetime | None, nanos: bool = False) -> str | None:
    if dt is None:
        return None
    s = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s + ("789Z" if nanos else "Z")


def order_view(oid: str, status: str, created, accepted=None, picked=None, delivered=None, cancelled=None,
               courier=None, customer="c-1", version=0, cancel_reason=None) -> dict:
    return {
        "id": oid, "status": status, "version": version, "customerId": customer, "customerName": "Customer",
        "merchant": MERCHANT, "dropoff": {"lat": 37.87, "lng": -122.25}, "courierId": courier,
        "items": [{"name": "Lunch", "quantity": 1}],
        "createdAt": iso(created), "updatedAt": iso(created), "statusChangedAt": iso(created),
        "acceptedAt": iso(accepted), "pickedUpAt": iso(picked), "deliveredAt": iso(delivered),
        "cancelledAt": iso(cancelled), "cancelReason": cancel_reason,
    }


class Envelopes:
    def __init__(self):
        self.next_id = 1
        self.records: list[tuple[str, str]] = []  # (key, value)

    def event(self, typ: str, at, order: dict, courier=None, event_id=None, schema_version=1, nanos=False, **extra):
        eid = event_id if event_id is not None else self.next_id
        if event_id is None:
            self.next_id += 1
        env = {
            "schemaVersion": schema_version, "eventId": eid, "type": typ, "occurredAt": iso(at, nanos),
            "aggregateType": "ORDER", "aggregateId": order["id"], "orderId": order["id"],
            "courierId": courier, "customerId": order["customerId"], "payload": {"order": order, **extra},
        }
        self.records.append((order["id"], json.dumps(env)))
        return env

    def raw(self, key: str, value: str):
        self.records.append((key, value))


def order_events() -> list[tuple[str, str]]:
    e = Envelopes()
    # A
    a = lambda **kw: order_view("A", courier=kw.pop("courier", None), **kw)  # noqa: E731
    e.event("ORDER_CREATED", t(0, 10), a(status="CREATED", created=t(0, 10)))
    e.event("ORDER_OFFERED", t(0, 11), a(status="OFFERED", created=t(0, 10)), courier="k1", offerId="o-1", distanceMeters=120.5, nanos=True)
    acc = e.event("ORDER_ACCEPTED", t(0, 30), a(status="ACCEPTED", created=t(0, 10), accepted=t(0, 30), courier="k1"), courier="k1")
    e.raw("A", json.dumps(acc))  # at-least-once redelivery of the same event id
    e.event("ORDER_PICKED_UP", t(2, 0), a(status="PICKED_UP", created=t(0, 10), accepted=t(0, 30), picked=t(2, 0), courier="k1"), courier="k1")
    e.event("ORDER_DELIVERED", t(5, 30), a(status="DELIVERED", created=t(0, 10), accepted=t(0, 30), picked=t(2, 0), delivered=t(5, 30), courier="k1"), courier="k1")
    # B
    b = lambda **kw: order_view("B", customer="c-2", courier=kw.pop("courier", None), **kw)  # noqa: E731
    e.event("ORDER_CREATED", t(1, 5), b(status="CREATED", created=t(1, 5)))
    e.event("ORDER_ACCEPTED", t(1, 50), b(status="ACCEPTED", created=t(1, 5), accepted=t(1, 50), courier="k2"), courier="k2")
    e.event("ORDER_PICKED_UP", t(3, 0), b(status="PICKED_UP", created=t(1, 5), accepted=t(1, 50), picked=t(3, 0), courier="k2"), courier="k2")
    e.event("ORDER_CANCELLED", t(4, 10), b(status="CANCELLED", created=t(1, 5), accepted=t(1, 50), picked=t(3, 0), cancelled=t(4, 10), courier="k2", cancel_reason="customer unreachable"), courier="k2", reason="customer unreachable", cancelledBy="DISPATCHER")
    # C (open forever)
    e.event("ORDER_CREATED", t(2, 20), order_view("C", "CREATED", t(2, 20)))
    # D (corrupted terminal payload)
    d = lambda **kw: order_view("D", customer="c-3", courier=kw.pop("courier", None), **kw)  # noqa: E731
    e.event("ORDER_CREATED", t(3, 0), d(status="CREATED", created=t(3, 0)))
    e.event("ORDER_ACCEPTED", t(3, 20), d(status="ACCEPTED", created=t(3, 0), accepted=t(3, 20), courier="k1"), courier="k1")
    e.event("ORDER_PICKED_UP", t(4, 0), d(status="PICKED_UP", created=t(3, 0), accepted=t(3, 20), picked=t(4, 0), courier="k1"), courier="k1")
    e.event("ORDER_DELIVERED", t(6, 0), d(status="DELIVERED", created=t(3, 0), accepted=t(2, 30), picked=t(4, 0), delivered=t(6, 0), courier="k1"), courier="k1")
    # garbage
    e.raw("junk", "this is not json")
    e.event("ORDER_TELEPORTED", t(0, 40), order_view("Z", "CREATED", t(0, 40)))
    e.event("ORDER_CREATED", t(0, 45), order_view("Y", "CREATED", t(0, 45)), schema_version=2)
    return e.records


def ping(courier: str, at, order=None, lat=37.871, lng=-122.259, speed=5.0, schema_version=1) -> tuple[str, str]:
    return courier, json.dumps({
        "schemaVersion": schema_version, "courierId": courier, "lat": lat, "lng": lng,
        "recordedAt": iso(at), "receivedAt": iso(at + timedelta(seconds=1)), "speedMps": speed,
        "headingDeg": None, "accuracyM": 5.0, "orderId": order,
    })


def pings() -> list[tuple[str, str]]:
    return [
        ping("k1", t(0, 5)), ping("k1", t(0, 20)), ping("k1", t(0, 35), "A"), ping("k1", t(0, 50), "A"),
        ping("k1", t(1, 10), "A"), ping("k1", t(1, 30), "A"), ping("k1", t(1, 50), "A"),
        ping("k2", t(1, 55), "B"),
        ping("k9", t(0, 30), lat=95.0),                 # bad coordinates
        ping("k9", t(0, 31), schema_version=2),         # unsupported schema
    ]


# A far-future event that pushes every watermark past the scenario so append-mode windows flush.
def sentinel_order(minute: int = 60) -> tuple[str, str]:
    e = Envelopes()
    e.next_id = 9000 + minute
    e.event("ORDER_CREATED", t(minute, 0), order_view(f"S{minute}", "CREATED", t(minute, 0)))
    return e.records[0]


def sentinel_terminal_order(minute: int = 60) -> list[tuple[str, str]]:
    """Created and delivered within the same minute. active_orders_minutely's watermark advances only
    with order *end* times, so flushing it needs a finished order, not just a created one."""
    e = Envelopes()
    e.next_id = 9500 + minute
    oid = f"S{minute}"
    e.event("ORDER_CREATED", t(minute, 0), order_view(oid, "CREATED", t(minute, 0)))
    e.event("ORDER_DELIVERED", t(minute, 30), order_view(oid, "DELIVERED", t(minute, 0), accepted=t(minute, 5),
                                                          picked=t(minute, 10), delivered=t(minute, 30), courier="k-sentinel"), courier="k-sentinel")
    return e.records


def sentinel_ping(minute: int = 60) -> tuple[str, str]:
    return ping("k-sentinel", t(minute, 0))
