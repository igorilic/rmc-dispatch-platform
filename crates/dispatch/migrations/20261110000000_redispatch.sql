-- Re-dispatch (M5): versions that say what they replaced and why, frozen loads, deliveries,
-- truck reports from telematics, and site delays.

ALTER TABLE plans
    ADD COLUMN previous_version BIGINT REFERENCES plans (version),
    ADD COLUMN triggers JSONB NOT NULL DEFAULT '[]',
    ADD COLUMN diff JSONB;

ALTER TABLE loads ADD COLUMN frozen BOOLEAN NOT NULL DEFAULT false;

-- Telematics reports. An older report never overwrites a newer one.
ALTER TABLE trucks
    ADD COLUMN status_at   TIMESTAMPTZ,
    ADD COLUMN lat         DOUBLE PRECISION,
    ADD COLUMN lon         DOUBLE PRECISION,
    ADD COLUMN position_at TIMESTAMPTZ,
    ADD CHECK ((lat IS NULL) = (lon IS NULL));

-- Written only by site delays, so a customer's next revision doesn't erase one.
ALTER TABLE orders ADD COLUMN delayed_until TIMESTAMPTZ;

-- A delivery exists once the plant starts batching a load; from then on the load is frozen.
CREATE TABLE deliveries (
    order_id   TEXT NOT NULL REFERENCES orders (id),
    sequence   INTEGER NOT NULL CHECK (sequence > 0),
    plant_id   TEXT NOT NULL REFERENCES plants (id),
    truck_id   TEXT NOT NULL REFERENCES trucks (id),
    volume_m3  DOUBLE PRECISION NOT NULL CHECK (volume_m3 > 0),
    ticket_no  TEXT NOT NULL,
    -- The earliest ticket event seen: when batching started, or as close as we know.
    started_at TIMESTAMPTZ NOT NULL,
    -- Delivery status → when it happened (domain::Delivery). The first report of each wins.
    events     JSONB NOT NULL,
    PRIMARY KEY (order_id, sequence)
);
CREATE INDEX deliveries_by_start ON deliveries (started_at);
