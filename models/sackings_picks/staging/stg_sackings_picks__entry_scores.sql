with

source as (

    select *
    from {{ source('sackings_picks', 'customerscore') }}

),

renamed as (

    select
        id as entry_score_id,
        customerid as user_id,
        contestid as contest_id,
        points,
        isactive as is_active,
        creationdate as created_at,
        cast(creationdate as date) as created_date,
        modifieddate as modified_at,
        cast(modifieddate as date) as modified_date,
        'sackings_picks' as source
    from source

)

select * from renamed