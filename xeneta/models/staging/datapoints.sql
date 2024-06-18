with source as (

    select * from {{ source( 'source_data','raw_datapoints') }}

),

renamed as (

    select
        D_ID as d_id,
        CREATED as created,
        ORIGIN_PID as origin_pid,
        DESTINATION_PID as destination_pid,
        VALID_FROM as valid_from,
        VALID_TO as valid_to,
        COMPANY_ID as company_id,
        SUPPLIER_ID as supplier_id,
        EQUIPMENT_ID as equipment_id

    from source

)

select * from renamed