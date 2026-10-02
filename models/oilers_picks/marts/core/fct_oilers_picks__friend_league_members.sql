with

members as (

    select *
    from {{ ref('stg_oilers_picks__friend_league_members') }}

),

friend_leagues as (

    select *
    from {{ ref('stg_oilers_picks__friend_leagues') }}

)

select
    members.friend_league_member_id,
    members.friend_league_id,
    members.user_id,

    members.client_id,
    members.tenant_id,
    members.tenant_name,
    members.game_type,

    coalesce(members.user_id = friend_leagues.owner_user_id, false) as is_league_owner,

    cast(
        convert_timezone('UTC', '{{ var("local_timezone") }}', members.joined_at) as date
    ) as joined_date_et,

    members.joined_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', members.joined_at) as joined_at_et

from members
left join friend_leagues
    on members.friend_league_id = friend_leagues.friend_league_id
