{{
    config(
        tags=['intermediate_models'], 
        alias = 'int_products_v',
        pre_hook = [use_warehouse('INTERMEDIATE_WH')]
    )
}}

with stg_product as(
    select * from {{ref('stg_product')}}
), 
stg_productcategory as(
    select * from {{ref('stg_productcategory')}}
), 
stg_subcategory as(
    select * from {{ref('stg_productsubcategory')}}
)

select sp.PRODUCTID, 
       sp.name as product_name, 
       sp.productnumber, 
       sp.color as product_color, 
       sp.class as product_class, 
       sst.name as product_subcategory_name, 
       spt.name as product_category_name, 
       sp.MODIFIEDDATE as product_updatedate
from stg_product sp
     left outer join stg_subcategory  sst on (sst.PRODUCTSUBCATEGORYID = sp.PRODUCTSUBCATEGORYID)
     left outer join stg_productcategory  spt on (spt.PRODUCTCATEGORYID = sst.PRODUCTCATEGORYID)