with

entries as (

    select *
    from {{ ref('stg_pln_arcade__entries') }}

),

games as (

    select *
    from {{ ref('stg_pln_arcade__game_providers') }}

),

users as (

    select *
    from {{ ref('stg_pln_arcade__users')}}

),

transactions as (

    select *
    from {{ ref('stg_pln_arcade__coin_transactions') }}

),

prizes as (

    select *
    from {{ ref('stg_pln_arcade__game_prizes') }}
    where prize_tier > 0

),

entry_prizes as (

    select
        entry_id,
        count(*) as prizes_won,
        sum(case when prize_type = 'coins' then prize_amount else 0 end) as coins_won,
        sum(case when prize_type = 'coins' then 1 else 0 end) as coin_prizes_won,
        sum(case when prize_type = 'voucher' then prize_amount else 0 end) as voucher_amount_won,
        sum(case when prize_type = 'voucher' then 1 else 0 end) as voucher_prizes_won
    from prizes
    group by 1

),

joined as (

    select
        entries.entry_id,
        entries.game_id,
        entries.user_id,
        entries.transaction_id,

        games.game_provider_name as game_name,

        users.username,
        users.email,

        abs(transactions.coin_amount) as coins_wagered,
        
        coalesce(entry_prizes.prizes_won,0) as prizes_won,
        coalesce(entry_prizes.coin_prizes_won,0) as coin_prizes_won,
        coalesce(entry_prizes.voucher_prizes_won,0) as voucher_prizes_won,
        coalesce(entry_prizes.coins_won,0) as coins_won,
        coalesce(entry_prizes.voucher_amount_won,0) as voucher_amount_won,
        case when entry_prizes.entry_id > 0 then true else false end as is_winner,

        entries.is_played,
        entries.created_date_et as entered_date_et,
        entries.created_date as entered_date
    
    from entries
    left join games
        on entries.game_id = games.game_provider_id
    left join users
        on entries.user_id = users.user_id
    left join transactions
        on entries.transaction_id = transactions.coin_transaction_id
    left join entry_prizes
        on entries.entry_id = entry_prizes.entry_id

)

select * from joined