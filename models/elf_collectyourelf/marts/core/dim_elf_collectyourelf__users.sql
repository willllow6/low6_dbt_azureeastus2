
select 
    user_id,
    total_coins,
    registration_date,
    registration_date_et,
    created_at,
    updated_at
from {{ ref('stg_elf_collectyourelf__game_users') }}
where game_id = 1