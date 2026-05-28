with

source as (

    select *
    from {{ source('penn', 'squads_contests') }}

),

renamed as (

    select

        ---------- ids
        id as tournament_id,
        tenant_id,
        promotion_id,

        ---------- strings
        name as tournament_name,
        description,
        status,

        ---------- booleans
        status not in ('completed', 'cancelled') as is_active,
        is_test,

        ---------- game type
        'squads' as game_type,

        ---------- timestamps
        cast(starts_at as timestamp_ntz) as starts_at,
        cast(ends_at as timestamp_ntz) as ends_at,
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source

)

select * from renamed
