with

private_leagues as (

    select * 
    from {{ ref('stg_sackings_picks__leagues') }}
    where is_active = 'True'

),

private_league_members as (

    select * 
    from {{ ref('stg_sackings_picks__league_memberships') }}
    where is_active = 'True'

),

users as (

    select * 
    from {{ ref('stg_sackings_picks__users') }}
    where is_active = 'True'

),

customer_scores as (

    select * 
    from {{ ref('stg_sackings_picks__entry_scores') }}
    where is_active = 'True'

),

customer_scores_agg as (

    select
        user_id,
        sum(points) as points_total
    from customer_scores
    group by 1

),

combined as (

    select
        plm.member_id,
        plm.league_id,
        plm.user_id,
        pls.league_name,
        u.username,
        cs.points_total
    from private_league_members as plm 
        inner join private_leagues as pls 
            on plm.league_id = pls.league_id
        inner join users as u 
            on plm.user_id = u.user_id 
        inner join customer_scores_agg as cs 
            on plm.user_id = cs.user_id
    where
        league_name is not null
        and username is not null
        and points_total is not null
        
),

final as (

    select
        *,
        rank() over(
                    partition by league_id
                    order by points_total desc
                ) as league_rank
    from combined
)

select * from final

