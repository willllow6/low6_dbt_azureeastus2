with 

users as (

    select * from {{ ref('stg_sackings_picks__users') }}

),

accounts as (

    select * from {{ ref('stg_sackings_picks__accounts') }}

),

final as (

    select
        u.*,
        a.account_name as app_name,
        a.is_active as is_active_account
    from users as u
        left join accounts as a 
            on u.account_id = a.account_id

)

select * from final where is_active_account = true