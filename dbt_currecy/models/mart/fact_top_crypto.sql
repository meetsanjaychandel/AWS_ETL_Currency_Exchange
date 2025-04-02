{{ config(materialized='table') }}

SELECT
    symbol,
    name,
    market_cap,
    current_price,
    price_change_percentage_24h
FROM {{ ref('stg_crypto') }}
ORDER BY market_cap DESC
