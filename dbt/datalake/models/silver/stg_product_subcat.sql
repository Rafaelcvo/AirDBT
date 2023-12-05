with source_data as (
    select productnumber, name from {{ source('adventureworks-gcp', 'product')}}
)

select * from source_data