with

users as (

    select * from {{ ref('INT_ELF_BLAST__USERS') }}

),

final as (

    select
        active_streak,
        highest_streak,
        count(*) as users
    from users
    where is_user_active = TRUE
    group by 1,2

)

select * from final