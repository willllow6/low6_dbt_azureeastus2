
select 
    challenge_id,
    challenge_name,
    challenge_description,
    is_enabled,
    created_at
from {{ ref('stg_elf_collectyourelf__challenges') }}
