with 

source as (

    select *
    from {{ source('sackings_picks', 'leagueheader') }}

),

renamed as (

    select
        id as league_id,
        customerid as user_id,
        stickerid as sticker_id,
        colourid as colour_id,
        sportstypeid as sports_type_id,
        leaguename as league_name,
        uniquecode as league_code,
        isactive as is_active,
        stickerimage as sticker_image,
        createdby as created_by,
        creationdate as created_at,
        cast(creationdate as date) as created_date,
        modifiedby as modified_by,
        modifieddate as modified_at,
        cast(modifieddate as date) as modified_date,
        'sackings_picks' as source
    from source

)

select * from renamed