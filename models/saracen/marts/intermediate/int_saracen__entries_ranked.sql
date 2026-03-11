select
    *,
    row_number() over(
        partition by sso_user_id
        order by entered_at_et
    ) as user_entry_number,
    row_number() over(
        partition by sso_user_id, game_type
        order by entered_at_et
    ) as user_game_entry_number
from {{ ref('int_saracen__entries_unioned') }}