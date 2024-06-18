with source as (

    select * from {{ ref('DE_casestudy_exchange_rates') }}

),

renamed as (

    select
        day as day,
        currency as currency,
        rate as rate

    from source

)

select * from renamed