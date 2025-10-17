with

source as (

    select *
    from {{ source('pln_arcade','rewards') }}

),

renamed as (

    select
        rewardid as reward_id,
        userid as user_id,
        rewardtype as reward_type,
        rewardamount as reward_amount,
        rewardsource as reward_source,
        isnew as is_new,
        updatedat as updated_at,
        cast(createdat as date) as created_date,
        cast(convert_timezone('UTC','America/New_York',createdat) as date) as created_date_et,
        createdat as created_at,
        convert_timezone('UTC','America/New_York',createdat) as created_at_et
    from source
)

select * from renamed