with

source as (

    select *
    from {{ source('du_trivia', 'users') }}

),

renamed as (

    select

        ----------  ids
        userid::varchar as user_id,
        'du' as client_id,
        'du' as tenant_id,
        serviceuserid as service_user_id,
        dureference as external_user_id,

        ---------- strings
        'DU' as tenant_name,
        'trivia' as game_type,
        case
            when dureference is not null
                then 'sso'
            else 'form'
        end as registration_type,
        username,
        nickname,

        ---------- booleans
        deletedat is null as is_active,

        ---------- timestamps
        createdat::timestamp_ntz as registered_at,
        updatedat::timestamp_ntz as updated_at,
        deletedat::timestamp_ntz as deleted_at

    from source

)

select * from renamed
