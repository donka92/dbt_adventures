{{
    config(
        tags=['intermediate_models'], 
        alias = 'int_customers_v',
        pre_hook = [use_warehouse('INTERMEDIATE_WH')]        
    )
}}

with stg_customer as(

    select CUSTOMERID, 
           PERSONID,
           STOREID
    from {{ ref('stg_customers') }}
),

stg_person as(

    select BUSINESSENTITYID,
         concat(coalesce(firstname, ''), ' ', coalesce(middlename, ''), ' ', coalesce(lastname, '')) as fullname
    from {{ref('stg_person')}}
)

select stc.CUSTOMERID,
       sp.BUSINESSENTITYID, 
       sp.fullname 
       
from stg_customer stc 
     left outer join stg_person sp on (sp.BUSINESSENTITYID = stc.PERSONID)