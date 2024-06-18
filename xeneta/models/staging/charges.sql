with source as (
    
    select * from {{ source( 'source_data','raw_charges') }}

),

renamed as (

    select distinct
        D_ID as d_id,
        CURRENCY as currency,
        CHARGE_VALUE as charge_value

    from source

)

select * from renamed