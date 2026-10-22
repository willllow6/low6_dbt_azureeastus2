with

source as (

    select *
    from {{ source('sackings_picks', 'customer') }}

),

renamed as (

    select
        id as user_id,
        tmid as tm_id,
        accountid as account_id,
        isactive as is_active,
        -- phonenumber as phone_number,
        username,
        creationdate as created_at,
        cast(creationdate as date) as created_date,
        convert_timezone('UTC','America/New_York',creationdate) as created_at_et,
        cast(created_at_et as date) as created_date_et,
        modifieddate as modified_at,
        cast(modifieddate as date) as modified_date,
        'sackings_picks' as source
    from source
    where tmid is not null

)

select * from renamed