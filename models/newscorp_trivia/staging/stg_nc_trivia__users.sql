with

source as (

    select *
    from {{ source('nc_trivia', 'users') }}

),

renamed as (

    select

        ----------  ids
        userid as user_id,
        serviceuserid as service_user_id,
        ssouserid as sso_user_id,

        ---------- strings
        username,
        nickname,

        ---------- numerics

        ---------- booleans

        ---------- dates

        ---------- timestamps
        createdat as created_at,
        updatedat as updated_at,
        deletedat as deleted_at

    from source

)

select * from renamed