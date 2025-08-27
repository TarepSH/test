#!/usr/bin/env python3
import json, os, time, signal, sys
from datetime import datetime, timezone, timedelta
from confluent_kafka import Consumer, KafkaException
import redis

BROKER      = os.getenv("BROKER", "localhost:29092")
TOPIC       = os.getenv("TOPIC", "engagement_enriched")
GROUP_ID    = os.getenv("GROUP_ID", f"redis-leaderboard-{int(time.time())}")
AUTO_OFFSET = os.getenv("AUTO_OFFSET", "earliest")
REDIS_URL   = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BUCKET_TTL  = int(os.getenv("BUCKET_TTL_SECONDS", "900"))
WINDOW_MIN  = int(os.getenv("WINDOW_MINUTES", "10"))

def mk_consumer():
    return Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": AUTO_OFFSET,
        "enable.partition.eof": False,
        "debug": "broker,topic,protocol,consumer"
    })

def bucket_key(dt: datetime) -> str:
    dt_utc = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return "zset:eng:minute:" + dt_utc.strftime("%Y%m%d%H%M")

def seconds_from_event(val: dict) -> float:
    if val.get("engagement_seconds") is not None:
        try: return float(val["engagement_seconds"])
        except Exception: pass
    if val.get("duration_ms") is not None:
        try: return float(val["duration_ms"]) / 1000.0
        except Exception: pass
    return 0.0

def parse_event_ts(val: dict) -> datetime:
    ts = val.get("event_ts")
    if isinstance(ts, str):
        try:
            if ts.endswith("Z"): ts = ts[:-1] + "+00:00"
            return datetime.fromisoformat(ts).astimezone(timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc)

running = True
def stop(*_):
    global running
    running = False
    print("\n[redis-leaderboard] Stopping...", flush=True)

def consume_loop():
    print(f"[redis-leaderboard] BROKER={BROKER} TOPIC={TOPIC} GROUP_ID={GROUP_ID} REDIS_URL={REDIS_URL}", flush=True)
    r = redis.from_url(REDIS_URL)
    try:
        pong = r.ping()
        print(f"[redis-leaderboard] Redis ping: {pong}", flush=True)
    except Exception as e:
        print(f"[redis-leaderboard] Redis connect error: {e}", flush=True)
        sys.exit(2)

    cons = mk_consumer()
    cons.subscribe([TOPIC])
    last_heartbeat = time.time()
    messages = 0

    try:
        while running:
            msg = cons.poll(1.0)
            now = time.time()
            if now - last_heartbeat >= 5:
                print(f"[redis-leaderboard] heartbeat… consumed={messages}", flush=True)
                last_heartbeat = now

            if msg is None:
                continue
            if msg.error():
                print(f"[kafka] ERROR: {msg.error()}", flush=True)
                continue

            try:
                key = msg.key().decode("utf-8") if msg.key() else None
            except Exception:
                key = None
            try:
                val = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[warn] bad JSON: {e} value={msg.value()!r}", flush=True)
                continue

            content_id = key or val.get("content_id")
            if not content_id:
                print("[warn] missing content_id; skip", flush=True)
                continue

            ev_dt = parse_event_ts(val)
            zkey  = bucket_key(ev_dt)
            score = seconds_from_event(val)

            pipe = r.pipeline()
            pipe.zincrby(zkey, score, content_id)
            pipe.expire(zkey, BUCKET_TTL)
            pipe.execute()
            messages += 1
            print(f"[redis] +{score:.2f}s -> {zkey} [{content_id}] (total {messages})", flush=True)
    finally:
        cons.close()
        print("[redis-leaderboard] Closed.", flush=True)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    consume_loop()
