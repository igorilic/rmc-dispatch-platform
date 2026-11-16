-- When dispatch recorded each delivery, on the world's clock (domain::clock). A delivery recorded
-- later than that clock allows was recorded before CLOCK_OFFSET_HOURS moved it back, whatever time
-- its tickets carry: a plant may report a batch ahead, on schedule, as the demo does.
-- A delivery from before this column counts as recorded when it started.
ALTER TABLE deliveries ADD COLUMN recorded_at TIMESTAMPTZ;
UPDATE deliveries SET recorded_at = started_at;
ALTER TABLE deliveries ALTER COLUMN recorded_at SET NOT NULL;
