
select
    registration_date,
    registration_date_et,
    count(*) as users
from {{ ref('dim_elf_collectyourelf__users') }}
group by 1,2