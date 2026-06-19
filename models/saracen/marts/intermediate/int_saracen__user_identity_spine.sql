with 

pickem_users as (

    select 
        sso_user_id, 
        user_id as pickem_user_id,
        username as pickem_username,
        created_date_et as pickem_created_date_et,
        created_at_et as pickem_created_at_et
    from {{ ref('stg_saracen_picks__users') }}
    where 1 = 0 -- picks on dev; remove when connected to prod

),

bracket_users as (

    select
        sso_user_id,
        user_id as bracket_user_id,
        username as bracket_username,
        created_date_et as bracket_created_date_et,
        created_at_et as bracket_created_at_et
    from {{ ref('int_saracen_bracket__users_unioned') }}
    qualify row_number() over (partition by sso_user_id order by created_at_et asc) = 1

)

select
    coalesce(g1.sso_user_id, g2.sso_user_id) as sso_user_id,
    g1.pickem_user_id,
    g2.bracket_user_id,
    g1.pickem_username,
    g2.bracket_username,
    g1.pickem_created_date_et,
    g2.bracket_created_date_et,
    g1.pickem_created_at_et,
    g2.bracket_created_at_et
from pickem_users g1
full outer join bracket_users g2 
    on g1.sso_user_id = g2.sso_user_id