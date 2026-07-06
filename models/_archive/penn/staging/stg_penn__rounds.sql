with

source as (

    select *
    from {{ source('penn', 'squads_rounds') }}

),

renamed as (

    select

        ---------- ids
        id as round_id,
        contest_id as tournament_id,

        ---------- strings
        slug,
        name as contest_name,
        reveal_type,
        status as contest_status,

        ---------- numerics
        sequence,
        reveal_count,

        ---------- booleans
        status not in ('completed', 'cancelled') as is_active,

        ---------- game type
        'squads' as game_type,

        ---------- timestamps
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source

)

select * from renamed
