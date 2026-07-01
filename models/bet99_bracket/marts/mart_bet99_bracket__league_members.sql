with

league_members as (

    select *
    from {{ ref('fct_bet99_bracket__league_members') }}

),

leagues as (

    select *
    from {{ ref('dim_bet99_bracket__leagues') }}

),

users as (

    select *
    from {{ ref('dim_bet99_bracket__users') }}

),

joined as (

    select
        league_members.league_member_id,
        league_members.league_id,
        league_members.user_id,
        users.sso_user_id,
        league_members.league_joined_date,
        league_members.league_joined_date_et,
        league_members.league_joined_at,

        leagues.league_name,
        leagues.league_type,
        leagues.league_created_date,
        leagues.league_created_date_et,

        users.first_name,
        users.last_name,
        users.email,
        users.username

    from league_members
    left join leagues
        on league_members.league_id = leagues.league_id
    left join users
        on league_members.user_id = users.user_id

)

select * from joined
