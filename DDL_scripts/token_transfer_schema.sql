-- Create the database if it doesn't exist and switch to it
CREATE DATABASE IF NOT EXISTS assignment;
USE DATABASE assignment;

-- Create the schema for token transfers and switch to it
CREATE SCHEMA IF NOT EXISTS token_transfers_schema;
USE SCHEMA token_transfers_schema;

-- Create the fact table for token transfers data
CREATE OR REPLACE TABLE fact_token_transfers (
      blockchain VARCHAR NOT NULL  
    , from_address VARCHAR             -- Address sending the token
    , to_address VARCHAR               -- Address receiving the token
    , token_address VARCHAR NOT NULL   -- Token contract address
    , raw_amount DOUBLE               -- Raw transfer amount (as recorded on-chain)
    , transaction_hash VARCHAR NOT NULL  
    , event_index NUMBER NOT NULL   -- Index of the event within the transaction
    , block_timestamp TIMESTAMP_NTZ         
    , block_number DOUBLE              -- Block number where the transaction was recorded
    , block_hash VARCHAR                -- Hash of the block
    , PRIMARY KEY (blockchain, transaction_hash, event_index)
);

-- Clustering the ez_token_transfers table (Final table) for speed.
ALTER TABLE ASSIGNMENT.TOKEN_TRANSFERS.EZ_TOKEN_TRANSFERS
  CLUSTER BY (block_timestamp);

