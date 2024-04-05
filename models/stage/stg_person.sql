{{
    config(
        transient=False
    )
}}
select src.* 
from {{source('STG_SOURCE','PERSON')}} src