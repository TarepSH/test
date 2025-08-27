# Copilot Instructions: Streaming Analytics Pipeline

## Big Picture Architecture
- This project is a local streaming analytics pipeline for engagement data, using Docker Compose to orchestrate Postgres, Kafka, Debezium, ClickHouse, Redis, and Python services.
- Data flows: Postgres (CDC tables) → Debezium (Kafka Connect) → Kafka topics (flat) → Python enrichment (Faust or custom consumer) → enriched Kafka topic → ClickHouse (warehouse) → Redis (real-time leaderboard) → Streamlit dashboard.
- Key directories/files:
  - `simulate_flow.py`: Seeds and simulates engagement events in Postgres.
  - `enrich_consumer.py` / `faust_app.py`: Enriches Kafka events, joins with content, outputs to enriched topic.
  - `redis_leaderboard_consumer.py`: Consumes enriched events, updates Redis sorted sets for leaderboard.
  - `streamlit-dashboard.py`: Visualizes analytics from ClickHouse and Redis.
  - `docker-compose.yml`: Defines all service containers and networking.
  - `connector.json`: Debezium connector config for CDC and topic transforms.
  - `init.sql`: Postgres schema and publication setup.

## Developer Workflows
- **Start stack:** `docker compose up -d` (see `docker-compose.yml` for service details)
- **Seed data:** Run `simulate_flow.py` to insert random content and engagement events into Postgres.
- **CDC/Connector:** Register Debezium connector with `connector.json` via Kafka Connect REST API.
- **Enrichment:** Run `enrich_consumer.py` or `faust_app.py` to join/transform events.
- **Warehouse:** Use ClickHouse built-in web UI (`http://localhost:8123`) or CLI for queries.
- **Leaderboard:** Run `redis_leaderboard_consumer.py` to maintain rolling top-N in Redis.
- **Dashboard:** Run `streamlit run streamlit-dashboard.py` for interactive analytics.

## Project-Specific Patterns
- Kafka topics use flat naming via Debezium transforms (`content.flat`, `engagement_events.flat`, `engagement_enriched`).
- Enrichment consumers expect content cache keyed by `content_id` and join engagement events with content metadata.
- Redis leaderboard uses minute-bucketed sorted sets (`zset:eng:minute:<YYYYMMDDHHMM>`) for rolling aggregation.
- ClickHouse ingestion uses Kafka engine tables and materialized views for type-safe analytics.
- All Python scripts use environment variables for host/port/topic configuration (see top of each script).

## Integration Points
- Postgres: CDC tables, publication, and Debezium role.
- Kafka: Topics for raw, enriched, and CDC events.
- Debezium: Connector config in `connector.json` (transforms, predicates, routing).
- ClickHouse: Analytics DB, Kafka table, materialized view.
- Redis: Real-time leaderboard, TTL-based buckets.
- Streamlit: Dashboard UI, queries ClickHouse and Redis.

## Example: Adding a New Event Type
- Update Postgres schema in `init.sql` and `engagement_events` table.
- Update enrichment logic in `enrich_consumer.py` and/or `faust_app.py`.
- Update dashboard queries in `streamlit-dashboard.py` if needed.

## Conventions
- Use UUIDs for content and user IDs.
- All time fields are UTC and ISO8601.
- Prefer environment variables for service endpoints.
- Use Docker Compose network for inter-service communication.

---

For questions or unclear patterns, check the README or ask for clarification. Please update this file if new workflows or conventions are added.
