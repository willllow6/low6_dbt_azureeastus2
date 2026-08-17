with

source as (

    select *
    from {{ source('olybet_casino', 'game') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as game_id,

        ---------- strings
        name as game_name,
        description as game_description,
        url as game_url,
        prize_scheme,

        ---------- numerics
        min_tiers,
        max_tiers,
        min_tickets,
        max_tickets,
        min_prizes,
        max_prizes,
        min_aux_prizes,
        max_aux_prizes,

        ---------- booleans
        deleted_at is not null as is_deleted,

        ---------- timestamps
        created_at::timestamp_ntz as created_at,
        deleted_at::timestamp_ntz as deleted_at

    from source

)

select * from renamed
