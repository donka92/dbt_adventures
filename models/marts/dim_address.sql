{{
    config(
        tags = ['full_load_models'] ,
        transient=False,
        pre_hook = [use_warehouse('COMPUTE_WH')]  
    )
}}

with source_data as(
    select src.* from {{ref('int_addresses')}} src
)

select 
    {{dbt_utils.generate_surrogate_key(['ADDRESSID'])}} as address_key,
    src.ADDRESSID, 
    src.ADRESS, 
    src.POSTALCODE,
    src.CITY, 
    src.STATEPROVINCECODE, 
    src.STATEPROVINCENAME, 
    src.TERRITORYID, 
    src.ISONLYSTATEPROVINCEFLAG, 
    src.COUNTRYREGIONCODE, 
    src.COUNTRYREGIONNAME
from source_data src