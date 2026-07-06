{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_oilers_picks', 'shared_oilers_picks__users', 'oilers_share') }}"
) }}

with

entries as (

    select * from {{ ref('stg_oilers_picks__users') }}

)

select * from entries