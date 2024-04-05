{% snapshot dim_products %}
{{config(
    target_database = 'DBT_ADVENTURES_DEV', 
    target_schema = 'DW', 
    transient=False,
    strategy = 'timestamp', 
    updated_at = 'product_updatedate', 
    unique_key = 'PRODUCTID',
    invalidate_hard_deletes  = True
)}}

with source_data as(
    select * from {{ref('int_products')}}
)
select {{dbt_utils.generate_surrogate_key(['PRODUCTID'])}} as product_key,
       src.PRODUCTID, 
       src.product_name, 
       src.productnumber, 
       src.product_color, 
       src.product_class, 
       src.product_subcategory_name, 
       src.product_category_name, 
       src.product_updatedate
from source_data src
{% endsnapshot %}