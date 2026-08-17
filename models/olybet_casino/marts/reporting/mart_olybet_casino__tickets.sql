with

tickets as (

    select *
    from {{ ref('fct_olybet_casino__tickets') }}

),

prizes as (

    select
        prize_id,
        prize_description,
        prize_image_url,
        prize_expires_at
    from {{ ref('dim_olybet_casino__prizes') }}

),

users as (

    select
        user_id,
        registered_at_et
    from {{ ref('dim_olybet_casino__users') }}

)

select
    tickets.ticket_id,
    tickets.entry_id,
    tickets.user_id,
    users.registered_at_et as user_registered_at_et,
    tickets.contest_id,
    tickets.contest_name,
    tickets.game_type,
    tickets.prize_id,
    tickets.prize_name,
    tickets.prize_type,
    tickets.prize_value,
    prizes.prize_description,
    prizes.prize_image_url,
    prizes.prize_expires_at,
    tickets.ticket_status,
    tickets.is_played,
    tickets.played_date,
    tickets.played_date_et,
    tickets.played_at,
    tickets.played_at_et,
    tickets.created_at,
    tickets.updated_at
from tickets
left join prizes
    on tickets.prize_id = prizes.prize_id
left join users
    on tickets.user_id = users.user_id
