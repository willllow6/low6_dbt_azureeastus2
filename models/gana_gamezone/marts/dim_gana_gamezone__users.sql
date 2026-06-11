with

users as (

    select * from {{ ref('stg_gana_gamezone__users') }}

),

enriched as (

    select
        user_id,
        clerk_user_id,
        client_id,
        tenant_id,
        tenant_name,
        email,
        first_name,
        last_name,
        trim(coalesce(first_name, '') || ' ' || coalesce(last_name, ''))    as full_name,
        image_url,
        registration_type,
        (email is not null)                                                 as has_email,
        registered_at,
        created_at,
        updated_at
    from users

)

select * from enriched
