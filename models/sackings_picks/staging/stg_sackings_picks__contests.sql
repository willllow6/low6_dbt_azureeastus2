with

source as (

    select *
    from {{ source('sackings_picks', 'contest') }}

),

renamed as (

    select
        id as contest_id,
        bannerimage as banner_image,
        prizepool as prize_pool,
        accountid as account_id,
        contesttype as contest_type,
        team2,
        stickerid as sticker_id,
        contestname as contest_name,
        sportstypeid as sports_type_id,
        contesttime as contest_time,
        isactive as is_active,
        iscontestlocked as is_contest_locked,
        isracelocked as is_race_locked,
        modifiedby as modified_by,
        contestdate as contest_date,
        contestimagemobile as contest_image_mobile,
        contestdescription as contest_description,
        statusid as status_id,
        ispublished as is_published,
        contestimagedesktop as contest_image_desktop,
        sequence,
        location,
        team1,
        createdby as created_by,
        entryfee as entry_fee,
        racemode as race_mode,
        bannerimageurl as banner_image_url,
        creationdate as created_at,
        cast(creationdate as date) as created_date,
        modifieddate as modified_at,
        cast(modifieddate as date) as modified_date,
        'sackings_picks' as source
    from source
    
)

select * from renamed