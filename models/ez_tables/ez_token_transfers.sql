{{ config(
    materialized='incremental',
    unique_key=['blockchain','transaction_hash', 'event_index'],
    on_schema_change='fail'
) }}

WITH ranked_transfers AS (
    SELECT
        ft.blockchain,
        ft.block_timestamp,
        ft.block_number,
        ft.block_hash,
        ft.transaction_hash,
        ft.event_index,
        ft.from_address,
        ft.to_address,
        ft.token_address,
        ft.raw_amount,
        ft.raw_amount / pow(10, pr.decimals) as amount,
        ft.raw_amount * pr.usd_price / pow(10, pr.decimals) as amount_usd,
        ROW_NUMBER() OVER (PARTITION BY ft.blockchain, ft.transaction_hash, ft.event_index ORDER BY ft.block_timestamp, ft.event_index DESC) as rn -- Ensure No duplication
        -- And as the table is incremental, it is not computationally heavy.
    FROM {{ ref('fact_token_transfers') }} ft
    LEFT JOIN {{ ref('fact_token_prices') }} pr 
        ON ( 
            date_trunc('hour', ft.block_timestamp) = date_trunc('hour', pr.timestamp) 
            AND ft.token_address = pr.token_address
            AND ft.blockchain = pr.blockchain
        )
    WHERE 1=1
    {% if is_incremental() %}
        AND ft.block_timestamp > (SELECT max(block_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT
    blockchain,
    block_timestamp,
    block_number,
    block_hash,
    transaction_hash,
    event_index,
    from_address,
    to_address,
    token_address,
    raw_amount,
    amount,
    amount_usd
FROM ranked_transfers
WHERE rn = 1
ORDER BY block_timestamp, event_index DESC