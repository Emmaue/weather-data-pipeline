WITH source AS (
    SELECT * FROM {{ source('crypto_raw', 'stg_bitcoin') }}
),

flattened AS (
    SELECT
        json_data:timestamp::TIMESTAMP_NTZ as timestamp,
        json_data:price_usd::FLOAT as price_usd,
        json_data:asset::STRING as asset,
        json_data:source::STRING as source_system,
        ingested_at
    FROM source
)

SELECT * FROM flattened
-- IDEMPOTENCY FIX:
-- Group by Timestamp (the event time), keep the latest ingestion
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY timestamp 
    ORDER BY ingested_at DESC
) = 1