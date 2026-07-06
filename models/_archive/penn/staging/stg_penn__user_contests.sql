with

source as (

    select *
    from {{ source('penn', 'squads_user_contests') }}

),

renamed as (

    select

        ---------- ids
        id as user_contest_id,
        user_id,
        contest_id as tournament_id,

        ---------- strings
        penn_session_id,
        platform,

        ---------- timestamps
        cast(welcome_seen_at as timestamp_ntz) as welcome_seen_at,
        cast(created_at as timestamp_ntz) as entered_at,
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source

)

select * from renamed
