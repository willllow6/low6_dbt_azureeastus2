with

users as (

    select * from {{ ref('STG_ELF_BLAST__STREAKS') }}

),

final as (

    select
        streak_created_date_et,
        streak_reached,
        count(*) as streaks 
    from users
    group by 1,2

)

select * from final