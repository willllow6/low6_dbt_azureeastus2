with

source as (

    select *
    from {{ source('bet365_uf', 'COIN_TRANSACTIONS') }}

),

renamed as (

    select

        ----------  ids
        CoinTransactionId as coin_transaction_id,
        TenantId as tenant_id,
        UserId as user_id,
        PairTransactionId as pair_coin_transaction_id,
        'bet365' as client_id,

        ---------- strings
        TransactionType as transaction_type,
        case
            when TransactionType = 0 and Reference = 'LEADERBOARD REWARD' then 'Leaderboard Reward'
            when TransactionType = 0 then 'Admin'
            when TransactionType = 2 then 'Friend Referral Reward'
            when TransactionType = 4 then 'Registration Reward'
            when TransactionType = 5 then 'Card Purchase'
            when TransactionType = 6 and PairTransactionId is not null then 'Card Sale'
            when TransactionType = 6 then 'Card Quick Sale'
            when TransactionType = 7 then 'Pack Purchase'
            when TransactionType = 8 then 'Login Reward'
            when TransactionType = 9 then 'Coin Purchase'
            when TransactionType = 10 then 'Challenge Reward'
            when TransactionType = 11 then 'Over/Under Reward'
            when TransactionType = 12 then 'Coin Swap'
            when TransactionType = 13 then 'Ticket Swap'
            when TransactionType = 15 then 'Card Training'
            when TransactionType = 16 then 'Profile Complete'
            when TransactionType = 17 then 'All Access Pass Purchase'
            when TransactionType = 18 then 'Division Leaderboard Reward'
            when TransactionType = 19 and Reference = 'Cursed Castle of Coins' then 'Cursed Castle of Coins Wager'
            when TransactionType = 19 and Reference in ('SuperShootout Play') then 'Super Shootout Wager'
            when TransactionType = 19 and Reference in ('HorseBlitz Play') then 'Horse Blitz Wager'
            when TransactionType = 19 and Reference in ('Plinko Play') then 'Plinko Wager'
            when TransactionType = 19 and Reference in ('Skull Island', 'SkullIsland') then 'Skull Island Wager'
            when TransactionType = 19 and Reference in ('Ball Pool', '8Ball Pool') then '8ball Wager'
            when TransactionType = 19 and Reference in ('Masters', 'MatchMasters') then 'Match Masters Wager'
            when TransactionType = 20 and Reference = 'Cursed Castle of Coins Prize' then 'Cursed Castle of Coins Payout'
            when TransactionType = 20 and Reference in ('SuperShootout Prize') then 'Super Shootout Payout'
            when TransactionType = 20 and Reference in ('HorseBlitz Prize') then 'Horse Blitz Payout'
            when TransactionType = 20 and Reference in ('Plinko Prize') then 'Plinko Payout'
            when TransactionType = 20 and Reference in ('Skull Island Prize', 'SkullIsland Prize') then 'Skull Island Payout'
            when TransactionType = 20 and Reference like 'Puck Billiards Prize%' then '8ball Payout'
            when TransactionType = 20 and Reference like 'Puck Match Prize%' then 'Match Masters Payout'
            when TransactionType = 21 then 'Simulation Contract Purchase'
            when TransactionType = 22 then 'Simulation Game Entry'
            when TransactionType = 23 then 'Simulation Season Entry'
            when TransactionType = 24 then 'Simulation Season Reward'
            when TransactionType = 25 then 'Simluation Game Reward'
            when TransactionType = 26 then 'Simluation Game Daily Reward'
            when TransactionType = 28 then 'Turbo Track Training'
            else 'Not Defined'
        end as coin_transaction_type,
        Reference as coin_transaction_reference,
        'fantasy' as game_type,

        ---------- numerics
        Amount as coin_amount,

        ---------- booleans
        case when Amount < 0 then true else false end as is_sink,
        case when transaction_type in ('4','5','6','7','8','9','15','16','17','19','28') then true else false end as is_user_triggered,

        ---------- dates
        cast(CreatedAt as date) as coin_transaction_created_date,

        ---------- timestamps
        CreatedAt as coin_transaction_created_at

    from source

)

select * from renamed
