-- silver_schema.sql

CREATE TABLE dim_customer (
    customer_key   INTEGER PRIMARY KEY,
    customer_id    TEXT NOT NULL,
    customer_name  TEXT,
    segment        TEXT,
    UNIQUE (customer_id)
);

CREATE TABLE dim_geography (
    geography_key  INTEGER PRIMARY KEY,
    country        TEXT NOT NULL,
    state          TEXT,
    city           TEXT,
    postal_code    TEXT,
    region         TEXT,
    UNIQUE (country, state, city, postal_code, region)
);

CREATE TABLE dim_product (
    product_key    INTEGER PRIMARY KEY,
    product_id     TEXT NOT NULL,
    product_name   TEXT,
    category       TEXT,
    subcategory    TEXT,
    UNIQUE (product_id)
);

CREATE TABLE dim_ship_mode (
    ship_mode_key  INTEGER PRIMARY KEY,
    ship_mode      TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_date (
    date_key   INTEGER PRIMARY KEY,
    full_date  DATE NOT NULL UNIQUE,
    year       INTEGER NOT NULL,
    month      INTEGER NOT NULL,
    day        INTEGER NOT NULL,
    quarter    INTEGER NOT NULL
);

CREATE TABLE fact_orders (
    order_key       INTEGER PRIMARY KEY,
    order_id        TEXT NOT NULL,
    customer_key    INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    order_date_key  INTEGER NOT NULL REFERENCES dim_date(date_key),
    ship_date_key   INTEGER REFERENCES dim_date(date_key),
    ship_mode_key   INTEGER NOT NULL REFERENCES dim_ship_mode(ship_mode_key),
    geography_key   INTEGER REFERENCES dim_geography(geography_key),
    UNIQUE (order_id)
);

CREATE TABLE fact_order_items (
    order_item_key  INTEGER PRIMARY KEY,
    order_key       INTEGER NOT NULL REFERENCES fact_orders(order_key),
    product_key     INTEGER NOT NULL REFERENCES dim_product(product_key),
    quantity        INTEGER,
    sales           NUMERIC(12,2),
    discount        NUMERIC(6,4),
    profit          NUMERIC(12,2),
    row_id          BIGINT, -- referencia al registro original de Bronce
    UNIQUE (order_key, product_key, row_id)
);

-- Índices para mejorar las consultas de análisis
CREATE INDEX idx_fact_orders_customer_key
    ON fact_orders (customer_key);

CREATE INDEX idx_fact_order_items_product_key
    ON fact_order_items (product_key);

CREATE INDEX idx_fact_orders_order_date_key
    ON fact_orders (order_date_key);

CREATE INDEX idx_dim_geography_region
    ON dim_geography (region);
