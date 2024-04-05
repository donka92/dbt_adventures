{{
    config(
        tags=['intermediate_models'], 
        alias = 'int_products_per_customer_v',
        pre_hook = [use_warehouse('INTERMEDIATE_WH')]        
    )
}}

with stg_salesorderdetail as(
    select * from {{ref('stg_salesorderdetail')}}
),
stg_salesorderheader as(
    select * from {{ref('stg_salesorderheader')}}
)

select
     so.customerid,
     sum(sd.productid) as sum_products
from  stg_salesorderheader so 
    inner join stg_salesorderdetail sd on (sd.salesorderid = so.salesorderid)
group by so.customerid