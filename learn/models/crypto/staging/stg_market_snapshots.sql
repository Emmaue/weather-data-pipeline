WITH source AS (
    SELECT * FROM {{ source('crypto_raw', 'stg_market_snapshots') }}
),

flattened AS (
    SELECT
        json_data:ingested_at::TIMESTAMP_NTZ as snapshot_at,
        json_data:rank::INTEGER as rank,
        json_data:coin_id::STRING as coin_id,
        json_data:symbol::STRING as symbol,
        json_data:name::STRING as name,
        json_data:price_usd::FLOAT as price_usd,
        json_data:market_cap::FLOAT as market_cap,
        json_data:volume_24h::FLOAT as volume_24h,
        json_data:pct_change_24h::FLOAT as pct_change_24h,
        ingested_at
    FROM source
)

SELECT * FROM flattened
-- IDEMPOTENCY FIX:
-- Group by Coin + Snapshot Time, and keep the latest ingestion
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY coin_id, snapshot_at 
    ORDER BY ingested_at DESC
) = 1