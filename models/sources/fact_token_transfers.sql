{{ config(
    materialized='view',
    database='ASSIGNMENT',
    schema='TOKEN_TRANSFERS_SCHEMA'
) }}
with transfers as 
(
    select * from {{ source('TRANSFER_SOURCE', 'TOKEN_TRANSFERS_TABLE') }}
)
select 

    blockchain ,
    block_timestamp ,
    block_number ,
    block_hash , 
    transaction_hash ,
    event_index ,
    from_address ,
    to_address ,
    token_address ,
    raw_amount 

from transfers 

