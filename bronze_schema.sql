-- bronze_schema.sql

-- 1) Crear base de datos bronze
CREATE DATABASE bronze;

-- 2) Conectarse a la BD bronze

-- 3) Crear tabla bronce
CREATE TABLE IF NOT EXISTS public.orders_bronze (
    row_id          BIGINT PRIMARY KEY,
    order_id        TEXT,
    order_date      DATE,
    ship_date       DATE,
    ship_mode       TEXT,
    customer_id     TEXT,
    customer_name   TEXT,
    segment         TEXT,
    country         TEXT,
    city            TEXT,
    state           TEXT,
    postal_code     TEXT,
    region          TEXT,
    product_id      TEXT,
    category        TEXT,
    subcategory     TEXT,
    product_name    TEXT,
    sales           NUMERIC(12,4),
    quantity        INTEGER,
    discount        NUMERIC(6,2),
    profit          NUMERIC(12,4),

    -- Metadatos de ingesta
    ingestion_ts    TIMESTAMPTZ DEFAULT NOW(),
    source_file     TEXT
);
