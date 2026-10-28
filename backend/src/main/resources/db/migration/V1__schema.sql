CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TYPE user_role AS ENUM ('CUSTOMER', 'COURIER', 'DISPATCHER');
CREATE TYPE order_status AS ENUM ('CREATED', 'OFFERED', 'ACCEPTED', 'PICKED_UP', 'DELIVERED', 'CANCELLED');
CREATE TYPE courier_status AS ENUM ('OFFLINE', 'ONLINE');
CREATE TYPE offer_status AS ENUM ('PENDING', 'ACCEPTED', 'EXPIRED', 'DECLINED', 'WITHDRAWN', 'REVOKED');

CREATE TABLE users (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email         text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    display_name  text NOT NULL,
    role          user_role NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE merchants (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name       text NOT NULL UNIQUE,
    location   geography(Point, 4326) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX merchants_location_gix ON merchants USING GIST (location);

CREATE TABLE couriers (
    user_id              uuid PRIMARY KEY REFERENCES users (id),
    status               courier_status NOT NULL DEFAULT 'OFFLINE',
    location             geography(Point, 4326),
    location_at          timestamptz,   -- device timestamp of the last-known position
    location_received_at timestamptz,   -- server time we last heard from this courier
    updated_at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX couriers_online_location_gix ON couriers USING GIST (location) WHERE status = 'ONLINE';

CREATE TABLE orders (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id       uuid NOT NULL REFERENCES users (id),
    merchant_id       uuid NOT NULL REFERENCES merchants (id),
    idempotency_key   text NOT NULL,
    request_hash      text NOT NULL,
    dropoff           geography(Point, 4326) NOT NULL,
    dropoff_note      text,
    items             jsonb NOT NULL DEFAULT '[]'::jsonb,
    status            order_status NOT NULL DEFAULT 'CREATED',
    courier_id        uuid REFERENCES couriers (user_id),
    version           bigint NOT NULL DEFAULT 0,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    status_changed_at timestamptz NOT NULL DEFAULT now(),
    accepted_at       timestamptz,
    picked_up_at      timestamptz,
    delivered_at      timestamptz,
    cancelled_at      timestamptz,
    cancel_reason     text,
    -- idempotent creation: same customer + same key => same order
    CONSTRAINT orders_idempotency_uq UNIQUE (customer_id, idempotency_key),
    -- unassigned states have no courier, assigned states always do (cancelled may be either)
    CONSTRAINT orders_courier_matches_status CHECK (
        status = 'CANCELLED' OR ((status IN ('CREATED', 'OFFERED')) = (courier_id IS NULL))
    )
);
-- exactly one active order per courier, enforced by the database
CREATE UNIQUE INDEX orders_one_active_per_courier ON orders (courier_id) WHERE status IN ('ACCEPTED', 'PICKED_UP');
CREATE INDEX orders_active_idx ON orders (status, status_changed_at) WHERE status NOT IN ('DELIVERED', 'CANCELLED');
CREATE INDEX orders_customer_idx ON orders (customer_id, created_at DESC);

-- State machine: the only legal transitions. Anything else is rejected by the trigger below.
CREATE TABLE order_transitions (
    from_status order_status NOT NULL,
    to_status   order_status NOT NULL,
    PRIMARY KEY (from_status, to_status)
);
INSERT INTO order_transitions (from_status, to_status) VALUES
    ('CREATED',   'OFFERED'),    -- assignment made an offer
    ('CREATED',   'ACCEPTED'),   -- a courier accepted an open order / dispatcher assigned directly
    ('CREATED',   'CANCELLED'),
    ('OFFERED',   'ACCEPTED'),
    ('OFFERED',   'CREATED'),    -- offer expired, declined or withdrawn -> back to the pool
    ('OFFERED',   'CANCELLED'),
    ('ACCEPTED',  'PICKED_UP'),
    ('ACCEPTED',  'CREATED'),    -- dispatcher unassigned / reassign
    ('ACCEPTED',  'CANCELLED'),
    ('PICKED_UP', 'DELIVERED'),
    ('PICKED_UP', 'CANCELLED');

CREATE FUNCTION orders_before_update() RETURNS trigger AS $$
BEGIN
    IF NEW.status <> OLD.status THEN
        IF NOT EXISTS (SELECT 1 FROM order_transitions t
                       WHERE t.from_status = OLD.status AND t.to_status = NEW.status) THEN
            RAISE EXCEPTION 'illegal order transition % -> % (order %)', OLD.status, NEW.status, OLD.id
                USING ERRCODE = 'CD001';
        END IF;
        NEW.status_changed_at := now();
    END IF;
    IF NEW.id <> OLD.id OR NEW.customer_id <> OLD.customer_id OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'order identity columns are immutable' USING ERRCODE = 'CD002';
    END IF;
    NEW.version := OLD.version + 1;
    NEW.updated_at := now();
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_before_update
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION orders_before_update();

CREATE TABLE order_offers (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    uuid NOT NULL REFERENCES orders (id),
    courier_id  uuid NOT NULL REFERENCES couriers (user_id),
    status      offer_status NOT NULL DEFAULT 'PENDING',
    distance_m  double precision,
    offered_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL,
    resolved_at timestamptz
);
CREATE UNIQUE INDEX order_offers_one_pending_per_order ON order_offers (order_id) WHERE status = 'PENDING';
CREATE UNIQUE INDEX order_offers_one_pending_per_courier ON order_offers (courier_id) WHERE status = 'PENDING';
CREATE INDEX order_offers_pending_expiry ON order_offers (expires_at) WHERE status = 'PENDING';
CREATE INDEX order_offers_history ON order_offers (order_id, courier_id, resolved_at);

-- Raw ping history (also streamed to Kafka as telemetry).
CREATE TABLE courier_locations (
    id          bigserial PRIMARY KEY,
    courier_id  uuid NOT NULL REFERENCES couriers (user_id),
    location    geography(Point, 4326) NOT NULL,
    recorded_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    speed_mps   real,
    heading_deg real,
    accuracy_m  real
);
CREATE INDEX courier_locations_courier_time ON courier_locations (courier_id, recorded_at DESC);

-- Append-only domain event log. Serves WebSocket replay (by id) and the Kafka outbox (published_at).
CREATE TABLE events (
    id             bigserial PRIMARY KEY,
    type           text NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id   uuid NOT NULL,
    order_id       uuid,
    courier_id     uuid,
    customer_id    uuid,
    payload        jsonb NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz
);
CREATE INDEX events_unpublished_idx ON events (id) WHERE published_at IS NULL;
CREATE INDEX events_customer_idx ON events (customer_id, id);
CREATE INDEX events_courier_idx ON events (courier_id, id);
