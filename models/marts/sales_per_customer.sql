{{
    config(
        tags = ['incremental_models'],
        materialized = 'incremental', 
        unique_key = ['customerid'],
        transient = false, 
        alias = 'fact_sales_per_customer', 
        pre_hook = [use_warehouse('COMPUTE_WH')]
    )}}

with sales_per_customer as (
    select * from {{ref('int_sales_per_customer')}}
), 
products_per_customer as (
    select * from {{ref('int_products_per_customer')}}
), 
int_customers as(
    select customerid, 
           BUSINESSENTITYID, 
           fullname
    from {{ ref('int_customers') }}
)

select  {{dbt_utils.generate_surrogate_key(['spc.customerid'])}} as customer_key,
       spc.customerid, 
       sc.fullname,
       spc.cnt_orders, 
       spc.totaldue, 
       pc.sum_products ,
       '{{invocation_id}}'  as batch_id, 
       current_timestamp() as batch_timestamp
from sales_per_customer spc
     left outer join products_per_customer pc on (pc.customerid = spc.customerid)
     left outer join int_customers sc on (sc.customerid = spc.customerid)

{% if is_incremental() %}
   where 
   ( (spc.customerid > (select max(customerid) from {{this}}))
   or 
    exists 
    (select 1 from {{this}} 
    where {{this}}.totaldue <>spc.totaldue 
    and {{this}}.customerid = spc.customerid
    )
   )
{% endif %}