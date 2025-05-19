#!/bin/bash
set -e

# Environment variables with defaults
export POSTGRES_USER=${POSTGRES_USER:-adm}
export POSTGRES_DB=${POSTGRES_DB:-database}

# Check if DB exists
if psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" \
    -tAc "SELECT 1 FROM pg_database WHERE datname='$POSTGRES_DB'" | grep -q 1; then
    echo "Database $POSTGRES_DB already exists"
else
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
        CREATE DATABASE $POSTGRES_DB;
EOSQL
fi

# Create schema and enable pgvector
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- Enable PGVector extension
    CREATE EXTENSION IF NOT EXISTS vector;

    -- Regions table
    DROP TABLE IF EXISTS regions CASCADE;
    CREATE TABLE regions (
        region_id   TEXT PRIMARY KEY,
        name_ar     TEXT,
        name_en     TEXT,
        region_embedding VECTOR(1536) -- We embed region name(s)
    );

    -- Governorates table
    DROP TABLE IF EXISTS governorates CASCADE;
    CREATE TABLE governorates (
        gov_id      TEXT PRIMARY KEY,
        region_id   TEXT REFERENCES regions(region_id),
        name_ar     TEXT,
        name_en     TEXT,
        latitude    DOUBLE PRECISION,  -- can be NULL
        longitude   DOUBLE PRECISION,  -- can be NULL
        gov_embedding VECTOR(1536)     -- We embed governorate name(s)
    );

    -- Alerts table (no embedding)
    DROP TABLE IF EXISTS alerts CASCADE;
    CREATE TABLE alerts (
        alert_id       INT PRIMARY KEY,
        alert_title    TEXT,
        alert_type_ar  TEXT,
        alert_type_en  TEXT,
        from_date      TIMESTAMP,
        to_date        TIMESTAMP,
        status_ar      TEXT,
        status_en      TEXT
        -- removed title_embedding
    );

    -- Hazards table (still has an embedding if you want to search by hazard text)
    DROP TABLE IF EXISTS hazards CASCADE;
    CREATE TABLE hazards (
        hazard_id             TEXT PRIMARY KEY,
        description_ar        TEXT,
        description_en        TEXT,
        description_embedding VECTOR(1536)
    );

    -- M:N relationship between alerts and hazards
    DROP TABLE IF EXISTS alert_hazards CASCADE;
    CREATE TABLE alert_hazards (
        alert_id   INT REFERENCES alerts(alert_id),
        hazard_id  TEXT REFERENCES hazards(hazard_id),
        PRIMARY KEY (alert_id, hazard_id)
    );

    -- M:N relationship between alerts and governorates
    DROP TABLE IF EXISTS alert_governorates CASCADE;
    CREATE TABLE alert_governorates (
        alert_id INT REFERENCES alerts(alert_id),
        gov_id   TEXT REFERENCES governorates(gov_id),
        PRIMARY KEY (alert_id, gov_id)
    );
EOSQL
