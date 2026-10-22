with

source as (

    select *
    from {{ source('sackings_picks', 'account') }}

),

renamed as (

    select
        id as account_id,
        splasscreenimage as splash_screen_image,
        sponserimage as sponsor_image,
        appbackgroundimage as app_background_image,
        bannerimagesmall as banner_image_small,
        collegename as college_name,
        tiebreakerbackgroundimage as tie_breaker_background_image,
        accountlogo as account_logo,
        youareinimage as you_are_in_image,
        sharebackgroundimage as share_background_image,
        novaapiagent as nova_api_agent,
        appmainmenuicon as app_main_menu_icon,
        bannerurlsmall as banner_url_small,
        apptitleimage as app_title_image,
        accountname as account_name,
        tagname as tag_name,
        sponserurl as sponsor_url,
        bannerimagebig as banner_image_big,
        popupbanneronsubmissionurl as popup_banner_on_submission_url,
        sponsername as sponsor_name,
        popupbanneronsubmission as popup_banner_on_submission,
        rssfeedlink as rss_feed_link,
        bannerurlbig as banner_url_big,
        isactive as is_active,
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