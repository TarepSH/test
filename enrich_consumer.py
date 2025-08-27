#!/usr/bin/env python3
import json, os, signal, sys, time
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, Producer, KafkaException
from prometheus_client import start_http_server, Counter, Histogram, Gauge

BROKER        = os.getenv("BROKER", "localhost:29092")
TOPIC_CONTENT = os.getenv("TOPIC_CONTENT", "content.flat")
TOPIC_ENGAGE  = os.getenv("TOPIC_ENGAGE",  "engagement_events.flat")
TOPIC_OUT     = os.getenv("TOPIC_OUT",     "engagement_enriched")
# Use a unique group so we actually read data (change this anytime to re-read from earliest)
GROUP_ID      = os.getenv("GROUP_ID",      f"py-enricher-{int(time.time())}")
AUTO_OFFSET   = os.getenv("AUTO_OFFSET",   "earliest")

BATCH_MAX     = int(os.getenv("BATCH_MAX", "500"))
FLUSH_SECS    = float(os.getenv("FLUSH_SECS", "1.0"))
METRICS_PORT  = int(os.getenv("METRICS_PORT", "8000"))

consumed_msgs    = Counter("enricher_consumed_messages_total", "Total messages consumed", ["topic"])
produced_msgs    = Counter("enricher_produced_messages_total", "Total messages produced")
processing_time  = Histogram("enricher_processing_seconds", "Per-message processing time (s)")
flush_count      = Counter("enricher_flush_total", "Number of producer flushes")
output_queue_len = Gauge("enricher_output_queue", "Buffered output records waiting to flush")

def q2(x):  # 2-dp string
    return str((Decimal(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def normalize_key(raw: Optional[bytes]) -> Optional[str]:
    """Debezium JSON keys can be primitive ('\"uuid\"') or struct ('{\"id\":\"uuid\"}'). Normalize to plain uuid string."""
    if raw is None:
        return None
    s = raw.decode("utf-8")
    # Try parse JSON; if it's an object with 'id', use that. If it's a JSON string, strip quotes.
    try:
        k = json.loads(s)
        if isinstance(k, dict) and "id" in k and isinstance(k["id"], str):
            return k["id"]
        if isinstance(k, str):
            return k
    except Exception:
        pass
    # Fallback: raw decoded string
    return s.strip('"')

def mk_consumer(group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": BROKER,
        "group.id": group_id,
        "auto.offset.reset": AUTO_OFFSET,
        "enable.partition.eof": False,
    })

def mk_producer() -> Producer:
    return Producer({"bootstrap.servers": BROKER})

running = True
def stop(*_):
    global running
    running = False
    print("\nShutting down…", flush=True)

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

def main():
    start_http_server(METRICS_PORT)
    print(f"[enricher] Metrics at :{METRICS_PORT}/metrics")
    print(f"[enricher] GROUP_ID={GROUP_ID} BROKER={BROKER}")

    consumer = mk_consumer(GROUP_ID)
    producer = mk_producer()
    consumer.subscribe([TOPIC_CONTENT, TOPIC_ENGAGE])

    content_cache: Dict[str, Dict[str, Any]] = {}
    buffered = 0
    last_flush = time.time()

    def on_delivery(err, msg):
        if err:
            print(f"[produce] ERROR: {err}", file=sys.stderr, flush=True)
        else:
            produced_msgs.inc()
            # Print a small confirmation so you see produces happening
            print(f"[produce] topic={msg.topic()} key={msg.key().decode() if msg.key() else None}")

    print(f"[enricher] Consuming from {TOPIC_CONTENT}, {TOPIC_ENGAGE} -> producing {TOPIC_OUT}")

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                if buffered and (time.time() - last_flush >= FLUSH_SECS):
                    producer.flush(5); flush_count.inc()
                    buffered = 0; output_queue_len.set(0); last_flush = time.time()
                continue
            if msg.error():
                raise KafkaException(msg.error())

            topic = msg.topic()
            key = normalize_key(msg.key())
            val = json.loads(msg.value().decode("utf-8"))
            consumed_msgs.labels(topic=topic).inc()

            t0 = time.time()
            if topic == TOPIC_CONTENT:
                if key:
                    content_cache[key] = {
                        "content_type":   val.get("content_type"),
                        "length_seconds": val.get("length_seconds")
                    }
                    # Optional: show cache fills
                    # print(f"[cache] {key} -> {content_cache[key]}")
            elif topic == TOPIC_ENGAGE:
                c = content_cache.get(key or "", None)
                duration_ms = val.get("duration_ms")
                engagement_seconds = None
                engagement_pct = None

                if duration_ms is not None:
                    sec = Decimal(duration_ms) / Decimal(1000)
                    engagement_seconds = q2(sec)
                    if c and isinstance(c.get("length_seconds"), int) and c["length_seconds"] > 0:
                        engagement_pct = q2(sec / Decimal(c["length_seconds"]))

                out = {
                    "content_id": key,
                    "user_id": val.get("user_id"),
                    "event_type": val.get("event_type"),
                    "event_ts": val.get("event_ts"),
                    "duration_ms": duration_ms,
                    "content_type": c.get("content_type") if c else None,
                    "length_seconds": c.get("length_seconds") if c else None,
                    "engagement_seconds": engagement_seconds,
                    "engagement_pct": engagement_pct
                }

                producer.produce(
                    TOPIC_OUT,
                    key=(key.encode("utf-8") if key else None),
                    value=json.dumps(out).encode("utf-8"),
                    on_delivery=on_delivery
                )
                buffered += 1
                output_queue_len.set(buffered)
                if buffered >= BATCH_MAX:
                    producer.flush(5); flush_count.inc()
                    buffered = 0; output_queue_len.set(0); last_flush = time.time()

            processing_time.observe(time.time() - t0)
            producer.poll(0)
    finally:
        try:
            consumer.close()
        finally:
            producer.flush(5)
            print("[enricher] Closed.", flush=True)

if __name__ == "__main__":
    main()
