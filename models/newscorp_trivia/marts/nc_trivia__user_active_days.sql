select distinct
    user_id,
    entered_date_aet
from {{ ref('nc_trivia__user_entries') }}