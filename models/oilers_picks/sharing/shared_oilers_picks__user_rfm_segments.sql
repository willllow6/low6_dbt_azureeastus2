{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_oilers_picks', 'shared_oilers_picks__user_rfm_segments', 'oilers_share') }}"
) }}

with

segments as (

    select * from {{ ref('oilers_picks__user_rfm_segments') }}

)

select * from segments