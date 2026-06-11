with

source as (

    select * from {{ source('gana_gamezone', 'countries') }}

),

renamed as (

    select

        ---------- ids
        id      as country_id,

        ---------- strings
        name    as country_name,
        code    as country_code,

        ---------- numerics
        odds,

        ---------- timestamps
        cast(created_at as timestamp_ntz)   as created_at,
        cast(updated_at as timestamp_ntz)   as updated_at

    from source

)

select * from renamed
