-- Master data, imported from deploy/seed at every start. Operational fields (truck and plant
-- status) are only set on first import, so later updates survive a restart.
CREATE TABLE customers (
    id      TEXT PRIMARY KEY,           -- our customer number
    name    TEXT NOT NULL,
    address JSONB NOT NULL
);

CREATE TABLE postal_codes (
    code TEXT PRIMARY KEY,
    lat  DOUBLE PRECISION NOT NULL,
    lon  DOUBLE PRECISION NOT NULL
);

CREATE TABLE sites (                    -- ship-tos
    id           TEXT PRIMARY KEY,      -- our ship-to number
    customer_id  TEXT NOT NULL REFERENCES customers (id),
    name         TEXT NOT NULL,
    address      JSONB,
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    access_notes TEXT NOT NULL DEFAULT '',
    CHECK ((lat IS NULL) = (lon IS NULL)),
    CHECK (address IS NOT NULL OR lat IS NOT NULL)
);

CREATE TABLE mix_designs (
    code                      TEXT PRIMARY KEY,
    max_minutes_from_batching INTEGER NOT NULL CHECK (max_minutes_from_batching > 0)
);

CREATE TABLE plants (
    id                     TEXT PRIMARY KEY,
    lat                    DOUBLE PRECISION NOT NULL,
    lon                    DOUBLE PRECISION NOT NULL,
    loading_bays           INTEGER NOT NULL CHECK (loading_bays > 0),
    batching_rate_m3_per_h DOUBLE PRECISION NOT NULL CHECK (batching_rate_m3_per_h > 0),
    opens                  TIME NOT NULL,
    closes                 TIME NOT NULL,
    status                 TEXT NOT NULL CHECK (status IN ('online', 'offline'))
);

CREATE TABLE trucks (
    id               TEXT PRIMARY KEY,
    home_plant       TEXT NOT NULL REFERENCES plants (id),
    drum_capacity_m3 DOUBLE PRECISION NOT NULL CHECK (drum_capacity_m3 > 0),
    status           TEXT NOT NULL CHECK (status IN (
        'available', 'loading', 'en_route', 'on_site', 'discharging', 'returning', 'out_of_service'))
);

-- Orders, kept in step with the EVENTS stream by the intake.
CREATE TABLE orders (
    id                 TEXT PRIMARY KEY,  -- <customer number>/<customer's order number>
    customer_id        TEXT NOT NULL REFERENCES customers (id),
    site_id            TEXT NOT NULL REFERENCES sites (id),
    mix                TEXT NOT NULL REFERENCES mix_designs (code),
    total_volume_m3    DOUBLE PRECISION NOT NULL CHECK (total_volume_m3 > 0),
    requested_start    TIMESTAMPTZ NOT NULL,
    pour_rate_m3_per_h DOUBLE PRECISION NOT NULL CHECK (pour_rate_m3_per_h > 0),
    max_gap_minutes    INTEGER NOT NULL CHECK (max_gap_minutes > 0),
    status             TEXT NOT NULL CHECK (status IN (
        'unconfirmed', 'pending', 'confirmed', 'in_progress', 'shipped', 'delivered', 'cancelled')),
    -- The source's time for this state. An older event never overwrites a newer one.
    occurred_at        TIMESTAMPTZ NOT NULL,
    source_revision    TEXT
);
CREATE INDEX orders_by_start ON orders (requested_start);

-- Every planning run is a new version, so earlier plans stay readable (and M5 can diff them).
CREATE TABLE plans (
    version     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL,
    horizon_end TIMESTAMPTZ NOT NULL,
    metrics     JSONB NOT NULL,
    unplanned   JSONB NOT NULL
);

CREATE TABLE loads (
    plan_version BIGINT NOT NULL REFERENCES plans (version) ON DELETE CASCADE,
    order_id     TEXT NOT NULL REFERENCES orders (id),
    sequence     INTEGER NOT NULL CHECK (sequence > 0),
    volume_m3    DOUBLE PRECISION NOT NULL CHECK (volume_m3 > 0),
    plant_id     TEXT NOT NULL REFERENCES plants (id),
    truck_id     TEXT NOT NULL REFERENCES trucks (id),
    batch_at     TIMESTAMPTZ NOT NULL,
    eta_on_site  TIMESTAMPTZ NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (plan_version, order_id, sequence)
);
