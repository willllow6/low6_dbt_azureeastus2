select distinct
    user_id,
    game_attempt_date_aet
from {{ ref('nc_matchup__derived_game_attempts') }}