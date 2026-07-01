with

league_memberships as (

    select *
    from {{ ref('mart_bet99_bracket__league_members') }}

),

agg_league_memberships as (

    select
        league_name,
        league_type,
        league_created_date_et,
        league_joined_date_et,
        count(*) as league_members
    from league_memberships
    group by 1, 2, 3, 4

)

select * from agg_league_memberships
