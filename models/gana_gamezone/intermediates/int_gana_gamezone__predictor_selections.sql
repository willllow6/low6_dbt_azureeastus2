with

group_predictions as (

    select * from {{ ref('stg_gana_gamezone__group_predictions') }}

),

group_contests as (

    select
        *,
        -- Position groups alphabetically (Group A = 0, Group B = 1, ...) to match prediction array index
        row_number() over (order by group_name) - 1  as group_index
    from {{ ref('stg_gana_gamezone__group_contests') }}

),

-- Outer flatten: one row per group within each user's prediction
flattened_groups as (

    select
        gp.entry_id,
        gp.user_id,
        gp.client_id,
        gp.tenant_id,
        gp.tenant_name,
        gp.game_type,
        gp.contest_id,
        grp.index                       as group_index,
        grp.value:country_order         as country_order,
        grp.value:third_place::boolean  as third_place,
        gp.entered_at                   as selected_at
    from group_predictions as gp,
    lateral flatten(input => gp.prediction) as grp

),

-- Inner flatten: one row per position within each group prediction
flattened_positions as (

    select
        fg.entry_id,
        fg.user_id,
        fg.client_id,
        fg.tenant_id,
        fg.tenant_name,
        fg.game_type,
        fg.contest_id,
        gc.group_id,
        pos.index + 1                   as predicted_position,
        pos.value::varchar              as selected_country_id,
        fg.third_place,
        fg.selected_at
    from flattened_groups as fg
    inner join group_contests as gc
        on fg.group_index = gc.group_index,
    lateral flatten(input => fg.country_order) as pos

)

select
    fp.entry_id || '-' || fp.group_id || '-' || fp.predicted_position::varchar  as selection_id,
    fp.entry_id,
    fp.user_id,
    fp.client_id,
    fp.tenant_id,
    fp.tenant_name,
    fp.game_type,
    fp.contest_id,
    fp.group_id,
    fp.predicted_position,
    fp.selected_country_id,
    fp.predicted_position <= 2
        or (fp.predicted_position = 3 and fp.third_place)  as is_predicted_to_progress,
    fp.selected_at
from flattened_positions as fp
