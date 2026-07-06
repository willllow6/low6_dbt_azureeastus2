with

source as (

    select *
    from {{ source('penn', 'squads_user_slots') }}

),

renamed as (

    select

        ---------- ids
        id as user_slot_id,
        user_round_id,
        revealed_player_id,

        ---------- strings
        status,
        revealed_rarity,

        ---------- numerics
        day_index,

        ---------- timestamps
        cast(reveal_start as timestamp_ntz) as reveal_start,
        cast(reveal_end as timestamp_ntz) as reveal_end,
        cast(revealed_at as timestamp_ntz) as revealed_at,
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source

)

select * from renamed
