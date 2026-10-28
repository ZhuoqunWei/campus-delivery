"""Spark schemas mirroring contracts/*.schema.json. Timestamps are read as strings and parsed
explicitly (see metrics.parse_ts) so that 6- or 9-digit fractional seconds never turn into nulls."""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

GEO = StructType([StructField("lat", DoubleType()), StructField("lng", DoubleType())])

ORDER_VIEW = StructType([
    StructField("id", StringType()),
    StructField("status", StringType()),
    StructField("version", LongType()),
    StructField("customerId", StringType()),
    StructField("customerName", StringType()),
    StructField("merchant", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("location", GEO),
    ])),
    StructField("dropoff", GEO),
    StructField("courierId", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("name", StringType()),
        StructField("quantity", IntegerType()),
    ]))),
    StructField("createdAt", StringType()),
    StructField("updatedAt", StringType()),
    StructField("statusChangedAt", StringType()),
    StructField("acceptedAt", StringType()),
    StructField("pickedUpAt", StringType()),
    StructField("deliveredAt", StringType()),
    StructField("cancelledAt", StringType()),
    StructField("cancelReason", StringType()),
])

ORDER_EVENT = StructType([
    StructField("schemaVersion", IntegerType()),
    StructField("eventId", LongType()),
    StructField("type", StringType()),
    StructField("occurredAt", StringType()),
    StructField("aggregateType", StringType()),
    StructField("aggregateId", StringType()),
    StructField("orderId", StringType()),
    StructField("courierId", StringType()),
    StructField("customerId", StringType()),
    StructField("payload", StructType([
        StructField("order", ORDER_VIEW),
        StructField("offerId", StringType()),
        StructField("distanceMeters", DoubleType()),
        StructField("reason", StringType()),
        StructField("cancelledBy", StringType()),
        StructField("assignedBy", StringType()),
        StructField("previousCourierId", StringType()),
    ])),
])

COURIER_LOCATION = StructType([
    StructField("schemaVersion", IntegerType()),
    StructField("courierId", StringType()),
    StructField("lat", DoubleType()),
    StructField("lng", DoubleType()),
    StructField("recordedAt", StringType()),
    StructField("receivedAt", StringType()),
    StructField("speedMps", DoubleType()),
    StructField("headingDeg", DoubleType()),
    StructField("accuracyM", DoubleType()),
    StructField("orderId", StringType()),
])

ORDER_EVENT_TYPES = {
    "ORDER_CREATED", "ORDER_OFFERED", "OFFER_EXPIRED", "OFFER_DECLINED", "OFFER_WITHDRAWN",
    "ORDER_ACCEPTED", "ORDER_PICKED_UP", "ORDER_DELIVERED", "ORDER_CANCELLED", "ORDER_UNASSIGNED",
}
TERMINAL_TYPES = {"ORDER_DELIVERED", "ORDER_CANCELLED"}
