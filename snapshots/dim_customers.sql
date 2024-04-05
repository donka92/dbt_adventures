{% snapshot dim_customers %}
{{config(
    target_database = 'DBT_ADVENTURES_DEV', 
    target_schema = 'DW', 
    transient=False,
    strategy = 'check', 
    check_cols=['fullname'],
    unique_key = 'CUSTOMERID',
    invalidate_hard_deletes  = True
)}}
with source_data as(
    select * from {{ref('int_customers')}}
)
select {{dbt_utils.generate_surrogate_key(['src.CUSTOMERID'])}} as customer_key,
       src.CUSTOMERID,
       src.BUSINESSENTITYID, 
       src.FULLNAME      
from  source_data src


{% endsnapshot %}