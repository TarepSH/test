#!/usr/bin/env python3
import json
import sys
import signal
from confluent_kafka import Consumer, KafkaException

BROKER = "localhost:29092"   # Use this for host access to Kafka in Docker
TOPICS = ["app.public.engagement_events", "app.public.content"]
GROUP_ID = "python-simulator"

running = True
def shutdown(sig, frame):
    global running
    running = False
    print("\nShutting down consumer...")

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

conf = {
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",   # start at beginning if no committed offset
}

consumer = Consumer(conf)

try:
    consumer.subscribe(TOPICS)
    print(f"Subscribed to {TOPICS} on {BROKER}")

    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        try:
            value = msg.value().decode("utf-8")
            parsed = json.loads(value)
            print(f"[{msg.topic()}] key={msg.key()} value=")
            print(json.dumps(parsed, indent=2))
        except Exception as e:
            print(f"[{msg.topic()}] decode error: {e}, raw={msg.value()}")

finally:
    consumer.close()
    print("Consumer closed.")
