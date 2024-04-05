{{
    config(
        tags = ['staging']
    )
}}
select src.* 
from {{source('STG_SOURCE','PRODUCT')}} src