with

users as (

    select *
    from {{ ref('stg_fanstake_rivals__users') }}

),

teams as (

    select *
    from {{ ref('stg_fanstake_rivals__teams') }}

),

joined as (

    select
        users.user_id,
        users.team_id,
        users.fanstake_id,
        users.username,
        users.display_name,
        users.user_role,
        teams.team_name,
        teams.team_sport,
        teams.team_league,
        users.registration_date,
        users.registration_date_et,
        users.registered_at
    from users
    left join teams 
        on users.team_id = teams.team_id

)

select * from joined