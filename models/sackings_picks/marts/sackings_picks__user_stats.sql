with

users as (

    select * from {{ ref('sackings_picks__users') }}

),

final as (

    select
        created_date_et as user_created_date_et,
        count(*) as users
    from users as u 
    group by 1
)

select * from final