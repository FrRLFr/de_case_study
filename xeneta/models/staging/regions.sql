with source as (

    select * from {{ ref('DE_casestudy_regions') }}

),

renamed as (

    select
        SLUG as slug_region_name,
        NAME as region_name,
        parent as parent

    from source

)

select * from renamed