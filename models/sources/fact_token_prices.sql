{{ config(
    materialized='view',
    database='ASSIGNMENT',
    schema='TOKEN_TRANSFERS_SCHEMA'
) }}

-- CTE for token price data from FACT_PRICE_TABLE
WITH token_prices AS (
    SELECT * 
    FROM {{ source('PRICE_SOURCE', 'FACT_PRICE_TABLE') }}  -- Source table with token price info
),
-- CTE for asset metadata from DIM_ASSET_METADATA
decimal_info AS (
    SELECT * 
    FROM {{ source('PRICE_SOURCE','DIM_ASSET_METADATA') }}  -- Contains symbol and decimal info for tokens
)

SELECT 
    tp.blockchain
  , tp.token_address
  , di.symbol           -- Token symbol from metadata
  , tp.timestamp        -- Timestamp of the price record
  , tp.usd_price        -- USD price from the price table
  , di.decimals         -- Decimal information for token conversion
  , tp.inserted_at       -- Record insertion timestamp
FROM token_prices tp 
LEFT JOIN decimal_info di 
  ON (tp.blockchain = di.blockchain AND tp.token_address = di.token_address)  -- Join matching blockchain and token address
