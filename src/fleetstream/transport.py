"""Kafka transport (works with Apache Kafka, Redpanda, Confluent Cloud or Azure Event Hubs' Kafka endpoint)."""
from __future__ import annotations

from collections.abc import Iterator

from .config import KafkaSettings
from .models import GpsEvent


class KafkaProducer:
    def __init__(self, cfg: KafkaSettings):
        from confluent_kafka import Producer

        self.p = Producer({"bootstrap.servers": cfg.bootstrap, "linger.ms": 20, "compression.type": "lz4",
                           "enable.idempotence": True, "acks": "all"})

    def send(self, topic: str, key: str, value: bytes) -> None:
        while True:
            try:
                self.p.produce(topic, key=key.encode(), value=value)
                break
            except BufferError:          # local queue full: let it drain
                self.p.poll(0.1)
        self.p.poll(0)

    def flush(self) -> None:
        self.p.flush(10)


class KafkaEventSource:
    """Consumes GPS events. Offsets are committed manually *after* results are stored (at-least-once)."""

    def __init__(self, cfg: KafkaSettings):
        from confluent_kafka import Consumer

        self.c = Consumer({"bootstrap.servers": cfg.bootstrap, "group.id": cfg.group_id,
                           "auto.offset.reset": "earliest", "enable.auto.commit": False})
        self.c.subscribe([cfg.raw_topic])

    def batches(self, max_batch: int = 500, timeout_s: float = 1.0) -> Iterator[list[GpsEvent]]:
        while True:
            msgs = self.c.consume(max_batch, timeout_s)
            events = []
            for m in msgs:
                if m.error():
                    continue
                try:
                    events.append(GpsEvent.from_json(m.value()))
                except (ValueError, TypeError):
                    pass                 # in production: route to a dead-letter topic
            yield events

    def commit(self) -> None:
        self.c.commit(asynchronous=False)

    def close(self) -> None:
        self.c.close()


def ensure_topics(cfg: KafkaSettings, partitions: int = 6) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": cfg.bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    new = [NewTopic(t, num_partitions=partitions, replication_factor=1)
           for t in (cfg.raw_topic, cfg.alerts_topic) if t not in existing]
    if new:
        for f in admin.create_topics(new).values():
            try:
                f.result()
            except Exception:
                pass
