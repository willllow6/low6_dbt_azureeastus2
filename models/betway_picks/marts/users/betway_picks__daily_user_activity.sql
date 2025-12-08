with

date_generator as (

    select dateadd(day, seq4(), '2025-10-15'::date) as date_day
    from table(generator(rowcount => 1000))  -- pick a safe upper bound
    WHERE date_day <= CURRENT_DATE

),

users as (

    select
        user_id,
        registration_date_et,
        region
    from {{ ref('betway_picks__users') }}

),

entries as (

    select
        user_id,
        entry_date_et,
        1 as is_active,
        COUNT(*) AS entries_count
    from {{ ref('betway_picks__entries') }}
    group by 1,2,3

),

user_days AS (
    -- Create the full set of user-day rows between reg date and today
    SELECT
        u.user_id,
        u.region,
        d.date_day,
        u.registration_date_et
    FROM users u
    JOIN date_generator d 
      ON d.date_day >= u.registration_date_et

)

SELECT
    c.user_id,
    c.date_day,
    c.region,

    -- registration flag
    CASE WHEN c.date_day = c.registration_date_et THEN 1 ELSE 0 END AS is_registration,

    -- active / entries
    COALESCE(e.is_active, 0) AS is_active,
    COALESCE(e.entries_count, 0) AS entries_count

FROM user_days c
LEFT JOIN entries e 
       ON c.user_id = e.user_id
      AND c.date_day = e.entry_date_et