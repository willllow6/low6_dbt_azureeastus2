with

user_challenge_progress as (

    select *
    from {{ ref('stg_elf_collectyourelf__user_challenge_progress') }}

),

user_product_progress as (

    select *
    from {{ ref('stg_elf_collectyourelf__user_product_progress') }}

),

challenge_products as (

    select
        challenge_id,
        count(*) as products
    from {{ ref('stg_elf_collectyourelf__challenge_products') }}
    group by 1

),

user_challenge_products as (

    select
        user_id,
        challenge_id,
        sum(case when is_found then 1 else 0 end) as products_found,
        max(product_found_date_et) as latest_product_found_date_et
    from user_product_progress
    group by 1,2

),

joined as (

    select 
        c.user_id,
        c.challenge_id,
        cp.products,
        p.products_found,
        p.products_found / cp.products as pct_complete,
        p.latest_product_found_date_et,
        c.is_completed,
        c.challenge_completion_date,
        c.challenge_completion_date_et,
        c.completed_at,
        c.created_at,
        c.updated_at
    from user_challenge_progress as c
    left join user_challenge_products as p
        on c.user_id = p.user_id
        and c.challenge_id = p.challenge_id
    left join challenge_products as cp 
        on c.challenge_id = cp.challenge_id

)

select * from joined