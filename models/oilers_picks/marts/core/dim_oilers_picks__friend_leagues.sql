with

friend_leagues as (

    select *
    from {{ ref('stg_oilers_picks__friend_leagues') }}

)

select
    friend_league_id,
    owner_user_id,
    league_team_id,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    friend_league_name,
    friend_league_code,
    friend_league_type,

    cast(
        convert_timezone('UTC', '{{ var("local_timezone") }}', created_at) as date
    ) as created_date_et,

    created_at

from friend_leagues
