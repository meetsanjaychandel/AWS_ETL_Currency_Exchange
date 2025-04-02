{{ config(materialized= 'incremental') }}

SELECT
    symbol,
    name,
    market_cap,
    current_price,
    price_change_percentage_24h
FROM {{ ref('fact_top_crypto') }}
ORDER BY market_cap DESC
