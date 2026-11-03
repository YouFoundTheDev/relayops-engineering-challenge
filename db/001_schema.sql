-- RelayOps schema. The postgres image runs this on first start.
-- Kept idempotent so it can also be re-applied to an older local database.

CREATE TABLE IF NOT EXISTS events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT        NOT NULL,
    type            TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending',
    attempts        INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at       TIMESTAMPTZ,
    locked_by       TEXT,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Columns added after the first release; IF NOT EXISTS keeps old volumes usable.
ALTER TABLE events ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE events ADD COLUMN IF NOT EXISTS locked_at       TIMESTAMPTZ;
ALTER TABLE events ADD COLUMN IF NOT EXISTS locked_by       TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS last_error      TEXT;

-- event_id is the idempotency key. This index is what makes concurrent
-- duplicate submissions safe.
CREATE UNIQUE INDEX IF NOT EXISTS events_event_id_key ON events (event_id);

CREATE INDEX IF NOT EXISTS events_due_idx ON events (status, next_attempt_at);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'events_status_check'
    ) THEN
        ALTER TABLE events
            ADD CONSTRAINT events_status_check
            CHECK (status IN ('pending', 'processing', 'delivered', 'failed'));
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id          BIGSERIAL PRIMARY KEY,
    event_pk    BIGINT      NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    attempt     INTEGER     NOT NULL,
    status_code INTEGER,
    error       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS delivery_attempts_event_pk_idx
    ON delivery_attempts (event_pk, attempt);