{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_oilers_picks', 'shared_oilers_picks__entries', 'oilers_share') }}"
) }}

with

entries as (

    select * from {{ ref('oilers_picks__entries') }}

)

select * from entries