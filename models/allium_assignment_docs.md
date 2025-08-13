{% docs assignment_overview %}
## Assignment Overview

The **Assignment** project is designed to consolidate and transform blockchain data into a production-ready analytics layer. It includes models that transform raw token transfers and token price data into clean, incremental models.

### Key Components
- **Data Sources:**  
  Data comes from raw token transfers and token price sources. These are joined on the token address and block timestamp (truncated to an hour) and enriched with value calculations.

- **Models and Transformations:**  
  Incremental models combine transfer data with price information. A unique key consisting of `blockchain`, `transaction_hash`, and `event_index` ensures each row is unique.
  
- **Deployment:**  
  All models are built in the `production_tables` schema in Snowflake for production-grade analytics. Tests ensure critical columns are not null.
  
### How to Use
- **Generate Documentation:**  
  Run `dbt docs generate` and then `dbt docs serve` to view and share this documentation.
  
- **Navigation:**  
  Use the interactive interface to explore models, sources, and dependencies.

{% enddocs %}
