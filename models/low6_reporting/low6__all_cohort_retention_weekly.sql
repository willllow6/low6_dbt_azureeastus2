with

unioned as (

select 
    game_id,
    game_name,
    game_type,
    client_id,
    source_schema,
    cast(source_database as varchar) as source_database,
    cast(tenant_name as varchar) as tenant_name,
    cohort_week,
    activity_week,
    weeks_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from {{ ref('agg_low6__cohort_retention_weekly') }}

union all

select 
    game_id,
    game_name,
    game_type,
    client_id,
    source_schema,
    cast(source_database as varchar) as source_database,
    cast(tenant_name as varchar) as tenant_name,
    cohort_week,
    activity_week,
    weeks_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from {{ source('archive','gcrw_awseuwest1') }}

union all

select 
    game_id,
    game_name,
    game_type,
    client_id,
    source_schema,
    cast(source_database as varchar) as source_database,
    cast(tenant_name as varchar) as tenant_name,
    cohort_week,
    activity_week,
    weeks_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from {{ source('archive','gcrw_awsuseast1') }}

union all

select 
    game_id,
    game_name,
    game_type,
    client_id,
    source_schema,
    cast(source_database as varchar) as source_database,
    cast(tenant_name as varchar) as tenant_name,
    cohort_week,
    activity_week,
    weeks_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from {{ source('archive','gcrw_awsuseast2') }}

union all

select 
    game_id,
    game_name,
    game_type,
    client_id,
    source_schema,
    cast(source_database as varchar) as source_database,
    cast(tenant_name as varchar) as tenant_name,
    cohort_week,
    activity_week,
    weeks_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from {{ source('archive','gcrw_awsapsoutheast2') }}

union all

select 
    game_id,
    game_name,
    game_type,
    client_id,
    source_schema,
    cast(source_database as varchar) as source_database,
    cast(tenant_name as varchar) as tenant_name,
    cohort_week,
    activity_week,
    weeks_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from {{ source('reporting','gcrw_awseuwest1') }}

union all

select 
    game_id,
    game_name,
    game_type,
    client_id,
    source_schema,
    cast(source_database as varchar) as source_database,
    cast(tenant_name as varchar) as tenant_name,
    cohort_week,
    activity_week,
    weeks_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from {{ source('reporting','gcrw_awsuseast1') }}

union all

select 
    game_id,
    game_name,
    game_type,
    client_id,
    source_schema,
    cast(source_database as varchar) as source_database,
    cast(tenant_name as varchar) as tenant_name,
    cohort_week,
    activity_week,
    weeks_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from {{ source('reporting','gcrw_azureuksouth') }}

union all

select 
    game_id,
    game_name,
    game_type,
    client_id,
    source_schema,
    cast(source_database as varchar) as source_database,
    cast(tenant_name as varchar) as tenant_name,
    cohort_week,
    activity_week,
    weeks_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from {{ source('reporting','gcrw_azureuaenorth') }}

)

select
    game_id,
    game_name,
    game_type,
    client_id,
    source_schema,
    cast(source_database as varchar) as source_database,
    cast(tenant_name as varchar) as tenant_name,
    cohort_week,
    activity_week,
    weeks_since_cohort,
    cohort_size,
    retained_users,
    retention_rate
from unioned