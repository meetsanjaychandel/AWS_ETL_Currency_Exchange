{{ config(
    unique_key='id'
) }}

WITH raw AS (
    SELECT * FROM {{ ref('ext_raw_crypto_data') }}
)

SELECT
    id,
    symbol,
    current_price,
    market_cap,
    total_volume,
    high_24h,
    low_24h,
    price_change_percentage_24h,
    CAST(last_updated AS TIMESTAMP) AS last_updated
FROM raw
WHERE last_updated IS NOT NULL
