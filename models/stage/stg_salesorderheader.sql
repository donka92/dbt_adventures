{{
    config(
        tags = ['staging']
    )
}}

select src.* 
from {{source('STG_SOURCE','SALESORDERHEADER')}} src