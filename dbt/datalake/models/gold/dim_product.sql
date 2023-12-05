with stg_product as (
    select * from {{ref('stg_product')}}
),

stg_product_subcat as (
    select * from {{ref('stg_product_subcat')}}
)