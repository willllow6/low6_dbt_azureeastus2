with source as (
    select * from {{ source('cfl_fantasy', 'users') }}
),

renamed as (
    select
        ---------- ids
        id as user_id,
        externalSub as external_user_id,
        'cfl' as client_id,
        'cfl' as tenant_id,

        ---------- strings
        'CFL' as tenant_name,
        'fantasy' as game_type,
        displayName as display_name,
        locale,

        ---------- booleans
        displayNameFilled as has_display_name_filled,
        isVerified as is_verified,
        case when left(externalSub,3) != 'bot' then true else false end as is_user,

        ---------- dates
        cast(convert_timezone('UTC', createdAt) as date) as registered_date,

        ---------- timestamps
        convert_timezone('UTC', createdAt)::timestamp_ntz as registered_at,
        convert_timezone('UTC', createdAt)::timestamp_ntz as created_at,
        convert_timezone('UTC', updatedAt)::timestamp_ntz as updated_at,

        ---------- derived
        'sso' as registration_type  -- all users are created via SSO (externalSub always present)

    from source
)

select * from renamed
