{{
    config(
        tags = ['incremental_models'],
        transient=False,
        materialized = 'incremental',
        incremental_strategy = 'merge', 
        unique_key = ['SALESORDERID', 'SALESORDERDETAILID'],
        pre_hook = [use_warehouse('COMPUTE_WH')]
                
    )
}}
-- post_hook = [hard_delete_fact_sales('dw','fact_sales')]
-- post_hook = "call DBT_ADVENTURES_DEV.DW.my_proc();"
with stg_salesorderheader as(

    select SALESORDERID,
           status as ORDER_STATUS, 
           CREDITCARDID,
           CUSTOMERID,
           SALESPERSONID,
           SHIPTOADDRESSID,
           ORDERDATE::date  ORDERDATE 
    from {{ref('stg_salesorderheader')}}
),
stg_salesorderdetail as(

    select 
       SALESORDERID,
       SALESORDERDETAILID,
       PRODUCTID,
       ORDERQTY,
       UNITPRICE,
       (unitprice * orderqty) as revenue
    from {{ref('stg_salesorderdetail')}}
)

select {{dbt_utils.generate_surrogate_key(['ssd.SALESORDERID', 'ssd.SALESORDERDETAILID'])}} as sales_key, 
       case when ssd.PRODUCTID is not null then {{dbt_utils.generate_surrogate_key(['ssd.PRODUCTID'])}} else '-1' end as product_key, 
       case when ssh.CUSTOMERID is not null then {{dbt_utils.generate_surrogate_key(['ssh.CUSTOMERID'])}}  else '-1' end as customer_key, 
       case when ssh.CREDITCARDID is not null then {{dbt_utils.generate_surrogate_key(['ssh.CREDITCARDID'])}} else '-1' end as credit_card_key, 
       case when ssh.ORDER_STATUS is not null then {{dbt_utils.generate_surrogate_key(['ssh.ORDER_STATUS'])}} else '-1' end as order_status_key, 
       case when ssh.ORDERDATE is not null then {{dbt_utils.generate_surrogate_key(['ssh.ORDERDATE'])}} else '-1' end as oderdate_key,
       case when ssh.SHIPTOADDRESSID is not null then {{dbt_utils.generate_surrogate_key(['ssh.SHIPTOADDRESSID'])}} else '-1' end as ship_address_key,  
       ssd.SALESORDERID, 
       ssd.SALESORDERDETAILID,
       ssd.ORDERQTY,
       ssd.UNITPRICE,
       ssd.revenue,
       ssh.ORDERDATE,
       '{{invocation_id}}'  as batch_id     
       
from stg_salesorderheader ssh
     inner join stg_salesorderdetail ssd on (ssd.SALESORDERID = ssh.SALESORDERID)

{% if is_incremental() %}
    where ssh.ORDERDATE > (select max(ORDERDATE) from {{this}})
{% endif %}     