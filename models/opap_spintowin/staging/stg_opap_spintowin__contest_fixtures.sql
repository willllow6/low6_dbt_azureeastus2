with

source as (

    select *
    from {{ source('opap_spintowin', 'contest_fixtures') }}

),

renamed as (

    select

        ----------  ids
        id as contest_fixture_id,
        contest_id,

        ---------- strings
        external_id as external_fixture_id,
        name as fixture_name,
        home_team,
        away_team,
        status as fixture_status,

        ---------- booleans

        ---------- dates

        ---------- timestamps
        start_time::timestamp_ntz as fixture_starts_at,
        created_at::timestamp_ntz as created_at

    from source

)

select * from renamed
