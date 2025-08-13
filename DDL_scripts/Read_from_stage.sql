CREATE OR REPLACE PROCEDURE load_latest_token_transfers()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  -- Step 1: List files and find the latest one (remains the same).
  LIST '@"AWS_ETHEREUM_DATABASE"."PUBLIC"."AWS_ETH_DATA"/logs/';

  LET latest_file_path VARCHAR;
  SELECT REPLACE("name", 's3://aws-public-blockchain/v1.0/eth/', '') INTO :latest_file_path
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  ORDER BY "name" DESC
  LIMIT 1;

  -- Step 2: Build the full MERGE query as a single dynamic statement.
  LET merge_query VARCHAR := '
      MERGE INTO AWS_ETHEREUM_DATABASE.TOKEN_TRANSFERS.FACT_TOKEN_TRANSFERS AS target
      USING (
          -- This is your source data from the file
          SELECT
              $1:address as contract_address,
              $1:block_number as block_number,
              $1:block_timestamp as block_timestamp,
              $1:log_index as event_index,
              $1:transaction_hash as transaction_hash,
              TO_NUMBER(LTRIM(SUBSTR($1:data, 3), ''0''), ''XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'') as token_amount
          FROM @"AWS_ETHEREUM_DATABASE"."PUBLIC"."AWS_ETH_DATA"/' || :latest_file_path || '
          (FILE_FORMAT => ''AWS_ETHEREUM_DATABASE.TOKEN_TRANSFERS.MY_PARQUET_FORMAT'')
          WHERE $1:address = ''0x68749665ff8d2d112fa859aa293f07a622782f38''
            AND $1:topics[0] = ''0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef''
            AND $1:data IS NOT NULL AND LTRIM(SUBSTR($1:data, 3), ''0'') != ''''
      ) AS source
      -- Use the composite unique key to match rows
      ON target.transaction_hash = source.transaction_hash AND target.event_index = source.event_index
      WHEN NOT MATCHED THEN
          -- If no match is found, insert the new row
          INSERT (
              contract_address,
              block_number,
              block_timestamp,
              event_index,
              transaction_hash,
              token_amount
          )
          VALUES (
              source.contract_address,
              source.block_number,
              source.block_timestamp,
              source.event_index,
              source.transaction_hash,
              source.token_amount
          );';

  -- Step 3: Execute the MERGE statement.
  EXECUTE IMMEDIATE :merge_query;

  -- Step 4: Return a success message.
  RETURN 'Merge complete. Rows affected (inserted or updated): ' || SQLROWCOUNT;
END;
$$;