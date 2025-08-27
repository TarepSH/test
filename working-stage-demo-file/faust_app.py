import faust
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Any, Dict

# Adjust broker for your setup:
# - If you enabled dual listeners in docker-compose: "kafka://localhost:29092"
# - If running inside the docker network: "kafka://kafka:9092"
BROKER_URL = "kafka://localhost:29092"

app = faust.App(
    "engagement-enricher",
    broker=BROKER_URL,
    store="memory://",  # use rocksdb:// for durability
)

# --------- Models ---------
class Content(faust.Record, serializer="json"):
    id: str
    slug: Optional[str] = None
    title: Optional[str] = None
    content_type: Optional[str] = None
    length_seconds: Optional[int] = None
    publish_ts: Optional[str] = None

class Engagement(faust.Record, serializer="json"):
    content_id: str
    user_id: Optional[str] = None
    event_type: Optional[str] = None
    event_ts: Optional[str] = None
    duration_ms: Optional[int] = None
    device: Optional[str] = None
    raw_payload: Optional[Dict[str, Any]] = None

class Enriched(faust.Record, serializer="json"):
    content_id: str
    user_id: Optional[str]
    event_type: Optional[str]
    event_ts: Optional[str]
    duration_ms: Optional[int]
    content_type: Optional[str]
    length_seconds: Optional[int]
    engagement_seconds: Optional[str]   # keep as string to preserve 2-dp formatting
    engagement_pct: Optional[str]       # keep as string to preserve 2-dp formatting

# --------- Topics ---------
topic_content = app.topic("content.flat", key_type=str, value_type=Content)
topic_engage  = app.topic("engagement_events.flat", key_type=str, value_type=Engagement)
topic_out     = app.topic("engagement_enriched", key_type=str, value_type=Enriched)

# --------- KTable (materialized view) ---------
content_tbl = app.Table("content_by_id", key_type=str, value_type=Content, partitions=1)

@app.agent(topic_content)
async def consume_content(stream):
    async for key, row in stream.items():
        # Key is content.id (string)
        content_tbl[key] = row

def q2(x: Decimal) -> str:
    """Format Decimal to exactly two decimals using HALF_UP."""
    return str(x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

@app.agent(topic_engage)
async def enrich(stream):
    async for key, e in stream.items():
        c = content_tbl.get(key)  # key == content_id
        engagement_seconds: Optional[str] = None
        engagement_pct: Optional[str] = None

        if e.duration_ms is not None:
            sec = Decimal(e.duration_ms) / Decimal(1000)
            engagement_seconds = q2(sec)

            if c is not None and c.length_seconds is not None and c.length_seconds > 0:
                pct = sec / Decimal(c.length_seconds)
                engagement_pct = q2(pct)

        enriched = Enriched(
            content_id=key,
            user_id=e.user_id,
            event_type=e.event_type,
            event_ts=e.event_ts,
            duration_ms=e.duration_ms,
            content_type=c.content_type if c else None,
            length_seconds=c.length_seconds if c else None,
            engagement_seconds=engagement_seconds,
            engagement_pct=engagement_pct,
        )
        await topic_out.send(key=key, value=enriched)

if __name__ == "__main__":
    app.main()
