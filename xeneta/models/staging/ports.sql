with source as (

    select * from {{ ref('DE_casestudy_ports') }}

),

renamed as (

    select distinct
        PID as pid,
        CODE as code,
        SLUG as slug_region,
        NAME as name,
        COUNTRY as country,
        COUNTRY_CODE as country_code

    from source

)

select * from renamed