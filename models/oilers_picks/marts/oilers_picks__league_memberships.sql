with 

leagues as ( 

    select * from {{ ref('stg_oilers_picks__leagues') }}

),

league_memberships as (

  select * from {{ ref('stg_oilers_picks__league_memberships') }}

),

join_league_data as (

    select
        league_memberships.membership_id,
        league_memberships.league_id,
        league_memberships.user_id,
        leagues.league_name,
        leagues.league_type,

        case
            when league_memberships.user_id = leagues.league_owner_user_id
                then true
            else false
        end as is_league_creator,

        league_memberships.league_joined_date,
        league_memberships.league_joined_date_et,
        leagues.created_date as league_created_date,
        leagues.created_date_et as league_created_date_et
  from league_memberships
    left join leagues
        on league_memberships.league_id = leagues.league_id
)

select * from join_league_data


