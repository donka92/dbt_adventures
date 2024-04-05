{{
    config(
        tags=['intermediate_models'],         
        alias = 'int_sales_per_customer_v',
        pre_hook = [use_warehouse('INTERMEDIATE_WH')]        
    )
}}

with source_data as (

    select CUSTOMERID, 
           count(salesorderid) as cnt_orders, 
           sum(totaldue)  as totaldue
    from {{ref('stg_salesorderheader')}} ssh 
    group by CUSTOMERID
)

select *
from source_data 
