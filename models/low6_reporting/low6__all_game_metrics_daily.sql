
with 


unioned as (

select *
from {{ ref('agg_low6__game_metrics_daily') }}

union all

select *
from {{ source('archive','gmd_awseuwest1') }}

union all

select *
from {{ source('archive','gmd_awsuseast1') }}

union all

select *
from {{ source('archive','gmd_awsuseast2') }}

union all

select *
from {{ source('archive','gmd_awsapsoutheast2') }}

union all

select *
from {{ source('reporting','gmd_awseuwest1') }}

union all

select *
from {{ source('reporting','gmd_awsuseast1') }}

union all

select *
from {{ source('reporting','gmd_azureuksouth') }}

union all

select *
from {{ source('reporting','gmd_azureuaenorth') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['client_id','game_id','game_name','tenant_name','source_database']) }} as game_client_key,
    *
from unioned