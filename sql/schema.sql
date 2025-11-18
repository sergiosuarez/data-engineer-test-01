-- Airbnb Analytics Dimensional Model
-- Schema: analytics
-- Author: Sergio Suarez
-- Date: 2025-11-18
-- Description: This script creates the dimensional model schema for Airbnb analytics,
-- including dimension and fact tables designed for efficient querying and analysis.
-- PostgreSQL syntax is used for compatibility with common data warehousing solutions.

CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics;


-- Dimension: Date
-- Grain: one record per date
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INTEGER PRIMARY KEY, -- YYYYMMDD format
    full_date           DATE NOT NULL UNIQUE,
    day_of_week         SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_name            VARCHAR(12) NOT NULL,
    week_of_year        SMALLINT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    month               SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name          VARCHAR(12) NOT NULL,
    quarter             SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year                SMALLINT NOT NULL,
    is_weekend          BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date (full_date);


-- Dimension: Property Type (Type 1)
-- Grain: one record Per property type
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_property_type (
    property_type_key   BIGSERIAL PRIMARY KEY,
    property_type_name  TEXT NOT NULL,
    property_category   TEXT,
    description         TEXT,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_property_type_name
    ON dim_property_type (property_type_name);


-- Dimension: Neighborhood (Type 1)
-- Grain: One record per neighorhood
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_neighborhood (
    neighborhood_key    BIGSERIAL PRIMARY KEY,
    neighborhood_name   TEXT NOT NULL,
    city                TEXT,
    state               TEXT,
    country             TEXT,
    geo_hash            TEXT,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_neighborhood_name
    ON dim_neighborhood (neighborhood_name, city, state);


-- Dimension: Host (SCD Type 2)
--  Grain: one recordd per host
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_host (
    host_key                BIGSERIAL PRIMARY KEY,
    host_id                 BIGINT NOT NULL,
    host_name               TEXT,
    host_since              DATE,
    host_response_time      TEXT,
    host_response_rate      NUMERIC(5,2), -- As percentage 0-100 or %
    host_is_superhost       BOOLEAN,
    host_listings_count     INTEGER,
    host_total_listings     INTEGER,
    host_verifications      TEXT,
    host_identity_verified  BOOLEAN,
    effective_from          TIMESTAMP NOT NULL,
    effective_to            TIMESTAMP,
    is_current              BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE dim_host
    ADD CONSTRAINT uq_dim_host_business_key
        UNIQUE (host_id, effective_from);

CREATE INDEX IF NOT EXISTS idx_dim_host_natural
    ON dim_host (host_id, is_current);


-- Dimension: Listing (SCD Type 2)
-- Grain: one record per listing
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_listing (
    listing_key            BIGSERIAL PRIMARY KEY,
    listing_id             BIGINT NOT NULL,
    host_key               BIGINT NOT NULL REFERENCES dim_host(host_key),
    property_type_key      BIGINT NOT NULL REFERENCES dim_property_type(property_type_key),
    neighborhood_key       BIGINT REFERENCES dim_neighborhood(neighborhood_key),
    listing_name           TEXT,
    room_type              TEXT,
    accommodates           INTEGER,
    bathrooms              NUMERIC(4,2),
    bedrooms               NUMERIC(4,2),
    beds                   NUMERIC(4,2),
    amenities_hash         TEXT,
    cancellation_policy    TEXT,
    minimum_nights         INTEGER,
    maximum_nights         INTEGER,
    instant_bookable       BOOLEAN,
    effective_from         TIMESTAMP NOT NULL,
    effective_to           TIMESTAMP,
    is_current             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE dim_listing
    ADD CONSTRAINT uq_dim_listing_business_key
        UNIQUE (listing_id, effective_from);

CREATE INDEX IF NOT EXISTS idx_dim_listing_natural
    ON dim_listing (listing_id, is_current);

CREATE INDEX IF NOT EXISTS idx_dim_listing_host
    ON dim_listing (host_key);

-- =============================================================
-- Fact: Listing Daily Metrics
-- Grain: one record per listing per date
-- =============================================================
CREATE TABLE IF NOT EXISTS fact_listing_daily_metrics (
    listing_daily_metrics_key BIGSERIAL PRIMARY KEY,
    date_key                  INTEGER NOT NULL REFERENCES dim_date(date_key),
    listing_key               BIGINT NOT NULL REFERENCES dim_listing(listing_key),
    host_key                  BIGINT NOT NULL REFERENCES dim_host(host_key),
    neighborhood_key          BIGINT REFERENCES dim_neighborhood(neighborhood_key),
    property_type_key         BIGINT REFERENCES dim_property_type(property_type_key),
    price                     NUMERIC(10,2),
    cleaning_fee              NUMERIC(10,2),
    security_deposit          NUMERIC(10,2),
    minimum_nights            INTEGER,
    maximum_nights            INTEGER,
    availability_30          INTEGER,
    availability_365         INTEGER,
    number_of_reviews         INTEGER,
    review_scores_rating      NUMERIC(5,2),
    occupancy_rate            NUMERIC(5,2),
    estimated_revenue         NUMERIC(12,2),
    price_tier                TEXT,
    created_at                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_listing_date
    ON fact_listing_daily_metrics (date_key, listing_key);

CREATE INDEX IF NOT EXISTS idx_fact_listing_host
    ON fact_listing_daily_metrics (host_key, date_key);

CREATE INDEX IF NOT EXISTS idx_fact_listing_neighborhood
    ON fact_listing_daily_metrics (neighborhood_key, date_key);

-- ==========================================================================================================================
-- Fact: Review Events (optional, one per review)
-- Grain: one record per review
-- Description: This fact table captures individual review events associated with listings. I think quality of reviews can impact listing performance.
-- ==========================================================================================================================
CREATE TABLE IF NOT EXISTS fact_review (
    review_key           BIGSERIAL PRIMARY KEY,
    review_id            BIGINT NOT NULL,
    listing_key          BIGINT NOT NULL REFERENCES dim_listing(listing_key),
    host_key             BIGINT NOT NULL REFERENCES dim_host(host_key),
    reviewer_id          BIGINT,
    reviewer_name        TEXT,
    date_key             INTEGER NOT NULL REFERENCES dim_date(date_key),
    review_scores_rating NUMERIC(5,2),
    comments_length      INTEGER,
    sentiment_score      NUMERIC(5,2),
    created_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_review_business
    ON fact_review (review_id);

CREATE INDEX IF NOT EXISTS idx_fact_review_listing_date
    ON fact_review (listing_key, date_key);
