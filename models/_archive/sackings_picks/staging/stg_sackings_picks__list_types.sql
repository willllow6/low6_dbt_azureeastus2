with

source as (

    select *
    from {{ source('sackings_picks', 'listtype') }}

),

renamed as (

    select 
        id as list_type_id,
        sequence,
        listtypevalue as list_type_value,
        isactive as is_active,
        listtypetext as list_type_text,
        listtypename as list_type_name,
        creationdate as created_at,
        cast(creationdate as date) as created_date,
        modifieddate as modified_at,
        cast(modifieddate as date) as modified_date,
        'sackings_picks' as source
    from source

)

select * from renamed