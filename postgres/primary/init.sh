#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    ALTER USER admin WITH REPLICATION PASSWORD 'admin';
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

    -- Create workload tables for pg_stat_statements and index advisor testing
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) UNIQUE NOT NULL,
        name VARCHAR(100),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        user_id INT,
        status VARCHAR(50) NOT NULL,
        total NUMERIC(10, 2),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        user_id INT,
        amount NUMERIC(12, 2) NOT NULL,
        category VARCHAR(100),
        is_flagged BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS events (
        id SERIAL PRIMARY KEY,
        user_id INT,
        type VARCHAR(50),
        payload TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    -- Seed baseline initial records
    INSERT INTO users (email, name)
    SELECT 'user_' || g || '@example.com', 'User ' || g
    FROM generate_series(1, 1000) g
    ON CONFLICT DO NOTHING;

    INSERT INTO transactions (user_id, amount, category, is_flagged)
    SELECT (random() * 999 + 1)::int, round((random() * 500)::numeric, 2), 
           (ARRAY['retail', 'travel', 'food', 'electronics'])[floor(random() * 4 + 1)],
           (random() < 0.05)
    FROM generate_series(1, 5000);
EOSQL
