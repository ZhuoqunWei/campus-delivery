"""Delivery analytics: Spark Structured Streaming over Campus Delivery's Kafka topics."""

SPARK_VERSION = "4.1.3"
SCALA_BINARY = "2.13"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY}:{SPARK_VERSION}"

ORDER_EVENTS_TOPIC = "order-events.v1"
COURIER_LOCATIONS_TOPIC = "courier-locations.v1"
