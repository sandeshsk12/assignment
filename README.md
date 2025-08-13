# Assignment

This is a data pipeline solution designed to ingest, process, and transform cryptocurrency market data and blockchain token transfers. The aim is to enrich a transfers table with usd amounts. The project leverages python for data collection, Snowflake for the data warehouse and dbt for modeling and transformation. This assignment demonstrates the ability to build scalable, maintainable data pipelines that combine token pricing, asset metadata, and blockchain transfer data.

For prototyping, we have restricted ourselves to develope the model for a single token, xAUT on Ethereum. This was done with keeping API limitation and storage cost in mind. However, the code has been written to handle multiple chains and tokens seamlessly. 

## Working Link
[Live streamlit app](https://appallium-6v8rbqbunzk3vhv3obezrd.streamlit.app/)

## Overview

The primary objectives of the project include:

- **Data Ingestion & Storage**:  
  Gather price data from external sources (ie, CoinGecko) and blockchain transfer data (e.g., via WebSocket feeds using getBlock.io as provider) and store them in Snowflake. The python scripts to retrive the data are hosted on pythonanywhere services.

- **Data Transformation with dbt**:  
  Use dbt to model, join, and transform raw data. This enables incremental processing of the final table. 

- **Data Integration**:  
  Combine asset metadata (such as token decimals and symbols) with token pricing and transaction details, enabling accurate computations such as USD conversion of raw token amounts.

## Tools and services used
Coingecko, getblock.io, pythonanywhere, Snowflake, DBT


## Architecture

1. **Data Sources**  
   - **Price Data**: Sourced from external price providers (e.g., CoinGecko) and stored in Snowflake tables.
     - APIs used: [Simple token price](https://docs.coingecko.com/v3.0.1/reference/simple-token-price) is used as we require price data. The python file is scheduled to run every hour. Historical data can be used if needed by adding it in dbt seeds folder. 
   - **Token Transfers**: Ingested from blockchain data streams and captured to dedicated Snowflake tables.

2. **Transformation Layer**  
   - **dbt Models**: SQL models are structured to transform raw inputs:
     - **Asset Metadata**: Managed in dimension tables for reference.
     - **Token Price & Transfers**: Joined with metadata to provide context (e.g., token decimals for accurate monetary computations).

3. **Data Warehouse**  
   - **Snowflake**: All transformed data (token_transfers.ez_token_transfers table) is stored in Snowflake and can be further used for analysis, reporting, or integration with BI tools.

[Image](https://ibb.co/S4qcNwHT)

## Repository Structure

```
assignment/
├── models/
│   ├── sources/              # Raw data ingestion models             
│   ├── ez_tables/            # Final model
│   └── schema.yml            # Model configurations and documentation
├── macros/                   # Custom dbt macros
├── analyses/                # Ad-hoc analysis queries
├── tests/                   # Data tests and quality checks
├── seeds/                   # Static seed data files (if any)
├── snapshots/               # Snapshot definitions for slowly changing dimensions
├── dbt_project.yml           # Main dbt project configuration
└── README.md                 # This file
```


## Future Improvements
- **Better token tracking source**
 Currently, token metadata is manually entered into Snowflake, but by leveraging DBT's seed functionality, we can automate this process. The idea is to place token data in a seed folder, which DBT can use to load token information into the database. This will allow us to fully automate the token price scraper, as we can dynamically query the metadata from the database without manual intervention. As a result, we can have a more efficient and scalable approach to tracking token metadata and ensure that the token list is always up-to-date.

- **Source_tables**
  Currently the source tables for token transfers only have generic tests. We could ensure that these are more robust by adding better tests given the time. 

- **Enhanced Data Monitoring**:  
  Integrate logging and alerting for data pipeline failures.
  
- **Expanded Data Sources**:  
  Incorporate additional data providers and enrich current datasets.
