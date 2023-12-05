with source_data as (
    select * from {{ source('adventureworks-gcp', 'product')}}
)

select * from source_data