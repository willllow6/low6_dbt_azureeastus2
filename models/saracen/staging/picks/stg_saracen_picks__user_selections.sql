with

source as (

    select *
    from {{ source('saracen_picks', 'user_selections') }}

),

users as (

    select *
    from {{ source('saracen_picks','users') }}

),

renamed as (

    select

        ----------  ids
        source.id as selection_id,
        source.user_id,
        split_part(users.sso_user_id,'_',1) as sso_user_id,
        source.pickem_id as contest_id,
        'PKM_' || source.pickem_id as contest_sk,
        source.question_id,
        source.option_id,

        ---------- strings
        -- bonus_selection_value,

        ---------- numerics
        try_to_number(substr(source.custom_value, 0, 5)) as tiebreaker_prediction,
        source.points_earned as points,

        ---------- booleans

        ---------- dates
        cast(source.created_at as date) as created_date,
        cast(convert_timezone('UTC','America/New_York',source.created_at) as date) as created_date_et,

        ---------- timestamps
        source.created_at,
        source.updated_at,
        convert_timezone('UTC','America/New_York',source.created_at) as created_at_et

    from source
    left join users 
        on source.user_id = users.id

)

select * from renamed
