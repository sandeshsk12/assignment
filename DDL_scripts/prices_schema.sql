CREATE DATABASE assignment ;

USE DATABASE assignment;

CREATE SCHEMA PRICES_SCHEMA;

CREATE TABLE dim_asset_metadata
(
    id VARCHAR NOT NULL, -- coingecko ID
    symbol VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    blockchain VARCHAR NOT NULL,
    token_address VARCHAR NOT NULL,
    decimals NUMBER NOT NULL,
    provider VARCHAR NOT NULL,
    PRIMARY KEY (blockchain, token_address)
);


INSERT INTO dim_asset_metadata (id, symbol, name, blockchain, token_address, decimals, provider)
VALUES 
    ('usd-coin', 'usdc', 'USDC', 'ethereum', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48','18', 'coingecko')
  , ('tether-gold', 'xaut', 'Tether Gold', 'ethereum', '0x68749665ff8d2d112fa859aa293f07a622782f38','6', 'coingecko')


select * from ASSIGNMENT.PRICES_SCHEMA.DIM_ASSET_METADATA


CREATE TABLE FACT_PRICE_TABLE 
(
      blockchain varchar not null
    , token_address varchar not null
    , timestamp timestamp not null 
    , usd_price double
    , inserted_at timestamp 
    , PRIMARY KEY (blockchain, token_address, timestamp)
)


select * from ASSIGNMENT.PRICES_SCHEMA.FACT_PRICE_TABLE
