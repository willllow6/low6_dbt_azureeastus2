with

users as (

    select *
    from {{ ref('stg_oilers_picks__users') }}

),

user_stats as (

    select
        created_date_et,
        has_consented_marketing,
        count(*) as signups
    from users
    group by 1,2

)

select * from user_stats