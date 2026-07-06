with

entries as (

    select *
    from {{ ref('pln_arcade__entries') }}

),

entry_stats as (

    select
        game_name,
        entered_date,
        entered_date_et,
        count(*) as entries,
        sum(case when is_played then 1 else 0 end) as entries_played,
        sum(coins_wagered) as coins_wagered,
        sum(prizes_won) as prizes_won,
        sum(coin_prizes_won) as coin_prizes_won,
        sum(case when coin_prizes_won > 0 then 1 else 0 end) as coin_payouts,
        sum(voucher_prizes_won) as voucher_prizes_won,
        sum(coins_won) as coins_won,
        sum(voucher_amount_won) as voucher_amount_won,
        sum(case when is_winner then 1 else 0 end) as winning_entries
    from entries
    group by 1,2,3

)

select * from entry_stats