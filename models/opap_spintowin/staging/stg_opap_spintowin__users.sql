with

source as (

    select *
    from {{ source('opap_spintowin', 'users') }}

),

renamed as (

    select

        ----------  ids
        id as user_id,
        external_user_id as sso_user_id,

        ---------- strings
        state as user_state,
        segment,

        ---------- booleans

        ---------- dates
        cast(created_at::timestamp_ntz as date) as registration_date,
        cast(convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', created_at::timestamp_ntz) as date) as registration_date_et,

        ---------- timestamps
        created_at::timestamp_ntz as registered_at,
        convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', created_at::timestamp_ntz) as registered_at_et,
        last_seen_at::timestamp_ntz as last_seen_at

    from source

)

select * from renamed
