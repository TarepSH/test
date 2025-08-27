#!/usr/bin/env python3
import os
import sys
import time
import random
import uuid
import json
import argparse
from datetime import datetime, timezone, timedelta

import psycopg2
from psycopg2.extras import execute_values

# ---------- Config ----------
def parse_args():
    p = argparse.ArgumentParser(
        description="Simulate content + engagement_events inserts into Postgres to drive Debezium CDC."
    )
    p.add_argument("--rate", type=float, default=5.0,
                   help="Average events per second (can be fractional). Default: 5")
    p.add_argument("--burst", type=int, default=50,
                   help="Max batch size per commit. Default: 50")
    p.add_argument("--ensure-content", type=int, default=10,
                   help="Ensure at least N content rows exist. Default: 10")
    p.add_argument("--jitter", type=float, default=0.3,
                   help="Randomness factor for inter-event timing (0..1). Default: 0.3")
    p.add_argument("--device", nargs="*", default=["ios","android","web-chrome","web-safari","roku"],
                   help="Device pool.")
    p.add_argument("--content-types", nargs="*", default=["podcast","newsletter","video"],
                   help="Allowed content types.")
    p.add_argument("--dsn", default=os.getenv("PG_DSN", "dbname=appdb user=app password=app host=localhost port=5432"),
                   help="Postgres DSN or set PG_DSN env var.")
    return p.parse_args()

# ---------- Helpers ----------
def connect(dsn):
    return psycopg2.connect(dsn)

def ensure_content(conn, min_rows, content_types):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM public.content;")
        (count,) = cur.fetchone()
        missing = max(0, min_rows - count)
        if missing == 0:
            return

        rows = []
        now = datetime.now(timezone.utc)
        for i in range(missing):
            cid = uuid.uuid4()
            ctype = random.choice(content_types)
            length = random.randint(60, 3600) if ctype != "newsletter" else None
            slug = f"seed-{ctype}-{cid.hex[:8]}"
            title = f"{ctype.capitalize()} {cid.hex[:6]}"
            rows.append((
                str(cid),
                slug,
                title,
                ctype,
                length,
                now
            ))

        execute_values(cur, """
            INSERT INTO public.content (id, slug, title, content_type, length_seconds, publish_ts)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
        """, rows)
    conn.commit()
    print(f"Seeded {missing} content rows")

def fetch_content_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM public.content;")
        return [r[0] for r in cur.fetchall()]

def pick_event_type():
    # Weighted: plays most common, then pause, click, finish
    #            play  pause click finish
    choices = ["play","pause","click","finish"]
    weights = [0.60, 0.20, 0.15, 0.05]
    return random.choices(choices, weights=weights, k=1)[0]

def mk_payload(device):
    return {
        "app_version": f"{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,9)}",
        "ip": ".".join(str(random.randint(1, 254)) for _ in range(4)),
        "device_meta": {"battery": random.randint(5,100), "mode": random.choice(["light","dark"])},
        "ab_bucket": random.choice(["A","B"])
    } if device.startswith("web") or device in ("ios","android") else {"platform":"tv"}

def generate_batch(content_ids, devices, batch_size):
    rows = []
    now = datetime.now(timezone.utc)
    for _ in range(batch_size):
        content_id = str(random.choice(content_ids))  # Convert to string
        user_id = str(uuid.uuid4())                  # Convert to string
        event_type = pick_event_type()
        device = random.choice(devices)
        duration = None
        if event_type in ("play","pause","finish"):
            duration = random.randint(500, 300000)  # ms
        payload = mk_payload(device)
        rows.append((
            content_id,                       # content_id (UUID as str)
            user_id,                          # user_id (UUID as str)
            event_type,                       # event_type
            now,                              # event_ts (TIMESTAMPTZ)
            duration,                         # duration_ms (nullable)
            device,                           # device
            json.dumps(payload)               # raw_payload (jsonb)
        ))
    return rows

def insert_engagements(conn, rows):
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO public.engagement_events
                (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
            VALUES %s
        """, rows)
    conn.commit()

# ---------- Main loop ----------
def main():
    print("Script started")  # Add this line
    args = parse_args()
    random.seed()

    try:
        conn = connect(args.dsn)
        conn.autocommit = False
    except Exception as e:
        print("Failed to connect to Postgres with DSN:", args.dsn, file=sys.stderr)
        raise

    ensure_content(conn, args.ensure_content, args.content_types)
    content_ids = fetch_content_ids(conn)
    if not content_ids:
        print("No content rows available; aborting.", file=sys.stderr)
        sys.exit(2)

    print(f"Loaded {len(content_ids)} content IDs. Starting stream @ ~{args.rate} ev/s … (Ctrl+C to stop)")
    # Convert rate to target sleep between single events
    base_interval = 1.0 / max(args.rate, 0.001)

    batch = []
    last_flush = time.time()

    try:
        while True:
            batch.append(generate_batch(content_ids, args.device, 1)[0])

            # If batch big enough or a second passed, flush
            now = time.time()
            if len(batch) >= args.burst or (now - last_flush) >= 1.0:
                insert_engagements(conn, batch)
                print(f"Inserted {len(batch)} events")
                batch.clear()
                last_flush = now

            # Sleep with jitter
            jitter = random.uniform(-args.jitter, args.jitter) * base_interval
            time.sleep(max(0.0, base_interval + jitter))

    except KeyboardInterrupt:
        if batch:
            insert_engagements(conn, batch)
            print(f"\nFlushed final {len(batch)} events on exit")
        conn.close()
        print("Stopped.")
    except Exception:
        conn.close()
        raise

if __name__ == "__main__":
    main()
