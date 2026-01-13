
select
    user_id,
    challenge_id,
    product_id,
    is_found,
    product_found_date,
    product_found_date_et,
    found_at,
    created_at,
    updated_at
from {{ ref('stg_elf_collectyourelf__user_product_progress') }}