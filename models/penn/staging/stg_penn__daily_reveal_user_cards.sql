with

source as (

    select *
    from {{ source('penn', 'daily_reveal_user_cards') }}

),

renamed as (

    select

        ---------- ids
        id as user_card_id,
        entry_id as daily_reveal_entry_id,
        card_id,

        ---------- strings
        rating,

        ---------- numerics
        position,

        ---------- booleans
        is_selected,

        ---------- timestamps
        cast(selected_at as timestamp_ntz) as selected_at,
        created_at

    from source

)

select * from renamed
