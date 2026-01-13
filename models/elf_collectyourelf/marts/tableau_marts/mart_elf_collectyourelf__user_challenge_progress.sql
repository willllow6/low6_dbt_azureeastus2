with

challenge_progress as (

    select *
    from {{ ref('fct_elf_collectyourelf__user_challenge_progress') }}

),

challenges as (

    select *
    from {{ ref('dim_elf_collectyourelf__challenges') }}

),

joined as (

    select
        challenge_progress.user_id,
        challenge_progress.challenge_id,

        challenge_progress.products,
        challenge_progress.products_found,
        challenge_progress.pct_complete,
        challenge_progress.is_completed,
        challenge_progress.challenge_completion_date,
        challenge_progress.challenge_completion_date_et,

        challenges.challenge_name,
        challenges.challenge_description
    from challenge_progress
    left join challenges
        on challenge_progress.challenge_id = challenges.challenge_id

),

progress_stats as (

    select
        challenge_name,
        challenge_description,
        challenge_completion_date_et,
        products,
        products_found,
        pct_complete,
        is_completed,
        count(*) as user_challenges
    from joined
    group by 1,2,3,4,5,6,7

)

select * from progress_stats