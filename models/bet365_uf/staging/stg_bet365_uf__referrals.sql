with

source as (

    select *
    from {{ source('bet365_uf', 'REFERRALS') }}

),

renamed as (

    select

        ----------  ids
        ReferralId as referral_id,
        'bet365' as client_id,
        UserId as user_id,
        referraluserid as referred_user_id,
        referreerewardrewardid as referree_reward_id,
        referrerrewardrewardid as referrer_reward_id,

        ---------- strings

        ---------- booleans

        ---------- dates

        ---------- timestamps
        CreatedAt

    from source

)

select * from renamed
