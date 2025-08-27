-- App schema
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Content catalogue ----------------------------------------------
CREATE TABLE IF NOT EXISTS content (
    id              UUID PRIMARY KEY,
    slug            TEXT UNIQUE NOT NULL,
    title           TEXT        NOT NULL,
    content_type    TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')),
    length_seconds  INTEGER,
    publish_ts      TIMESTAMPTZ NOT NULL
);

-- Raw engagement telemetry ---------------------------------------
CREATE TABLE IF NOT EXISTS engagement_events (
    id           BIGSERIAL PRIMARY KEY,
    content_id   UUID REFERENCES content(id),
    user_id      UUID,
    event_type   TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')),
    event_ts     TIMESTAMPTZ NOT NULL,
    duration_ms  INTEGER,
    device       TEXT,
    raw_payload  JSONB
);

-- Debezium role for logical replication
DO $$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbz') THEN
      CREATE ROLE dbz WITH LOGIN PASSWORD 'dbz' REPLICATION;
   END IF;
END$$;

GRANT CONNECT ON DATABASE appdb TO dbz;
GRANT USAGE ON SCHEMA public TO dbz;
GRANT SELECT ON TABLE public.content, public.engagement_events TO dbz;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbz;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dbz;

-- Logical replication publication (Debezium will use pgoutput)
DROP PUBLICATION IF EXISTS dbz_publication;
CREATE PUBLICATION dbz_publication
  FOR TABLE public.content, public.engagement_events;

-- Optional seed data to test quickly
INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
VALUES
  (uuid_generate_v4(), 'intro-pod', 'Intro to Streaming', 'podcast', 1800, now()),
  (uuid_generate_v4(), 'daily-newsletter', 'Daily Newsletter 001', 'newsletter', NULL, now())
ON CONFLICT DO NOTHING;
