-- Compatibility view for the Oilers Picks Tableau dashboard. Repoint the
-- dashboard to rpt_oilers_picks__registrations_daily, then delete this model.

with

registrations as (

    select *
    from {{ ref('rpt_oilers_picks__registrations_daily') }}

)

select
    registration_date_et as created_date_et,
    marketing_consent as has_consented_marketing,
    registrations as signups

from registrations
