{{
    config(
        tags = ['staging'],      
        pre_hook = [use_warehouse('COMPUTE_WH')],   
        post_hook= [add_tag('staging', 'stg_creditcard')]
    )
}}

select src.* 
from {{source('STG_SOURCE','CREDITCARD')}} src