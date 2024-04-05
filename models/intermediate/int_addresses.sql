{{
    config(
        tags=['intermediate_models'], 
        alias = 'int_addresses_v',
        materialized ='ephemeral',
        pre_hook = [use_warehouse('INTERMEDIATE_WH')]      
    )
}}
with stg_address as(
    select * from {{ref('stg_address')}}
),
stg_province as (
    select * 
    from {{ref('stg_stateprovince')}}
),
stg_countryregion as(
    select * 
    from {{ref('stg_countryregion')}}
)
select  adr.addressid, 
       case when adr.addressline2 is not null then concat(concat(adr.addressline1, ';'), addressline2) 
           else adr.addressline1 
       end as adress ,
       adr.POSTALCODE,
       adr.city,       
       stp.STATEPROVINCECODE,
       stp.name as STATEPROVINCENAME,
       stp.TERRITORYID,
       stp.ISONLYSTATEPROVINCEFLAG,
       ctr.COUNTRYREGIONCODE,
       ctr.name as COUNTRYREGIONNAME
from stg_address adr
     left outer join stg_province stp on (stp.STATEPROVINCEID = adr.STATEPROVINCEID)
     left outer join stg_countryregion ctr on (ctr.COUNTRYREGIONCODE = stp.COUNTRYREGIONCODE)