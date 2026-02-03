with 

source as (

    select *
    from {{ source('fanstake_rivals', 'weekly_periods') }}

),

renamed as (

    select
        id as weekly_period_id, -- primary key
        name as weekly_period_name,
        sport,
        season,
        week_number,
        status,
        image,
        league,
        metadata, -- variant/json
        start_date,
        cast(convert_timezone('UTC', 'America/New_York', to_timestamp_ntz(start_date)) as date) as start_date_et,
        end_date,
        created_at,
        updated_at -- last modified timestamp
    from source

)


select * from renamed