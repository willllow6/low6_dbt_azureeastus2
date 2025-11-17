with 

league_users as (

    select * from {{ ref('oilers_picks__league_memberships') }}

),

league_membership_stats as (
  
    select 
        league_name, 
        league_type,
        league_joined_date_et,
        league_created_date_et,
        count(user_id) as league_members
    from league_users
    group by 1,2,3,4

)

select * from league_membership_stats