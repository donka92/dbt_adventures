{{
    config(
        tags = ['dynamic_table_models'],        
        materialized = 'dynamic_table',
        snowflake_warehouse = 'COMPUTE_WH',
        target_lag = '60 seconds'
    )
}}
with source_data as(
    select * from {{ref('stg_creditcard')}}
)

select  {{dbt_utils.generate_surrogate_key(['stc.CREDITCARDID'])}}  as creditcard_key,
       stc.CREDITCARDID, 
       stc.CARDTYPE, 
       stc.expyear, 
       stc.modifieddate, 
       stc.cardnumber
from source_data stc