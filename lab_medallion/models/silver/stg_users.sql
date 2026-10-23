with src as (
    select * from {{ source('bronze', 'users') }}
),

first_orders as (
    select user_id, min(cast(ordered_at as date)) as first_order_date
    from {{ ref('stg_orders') }}
    group by user_id
),

cleaned as (
    select
        trim(uuid)                      as user_id,
        trim(username)                  as username,
        lower(trim(username))           as username_normalized,
        trim(name)                      as name,
        upper(trim(sex))                as sex,
        lower(trim(mail))               as email,
        try_cast(birthdate as date)     as birthdate_raw,
        replace(replace(address, chr(10), '|'), '\n', '|') as address_raw
    from src
    where uuid is not null
),

checked as (
    select
        c.*,
        (c.birthdate_raw is not null
         and (f.first_order_date is null or c.birthdate_raw <= f.first_order_date)) as birthdate_is_valid
    from cleaned c
    left join first_orders f using (user_id)
)

select
    user_id,
    username,
    username_normalized,
    name,
    sex,
    email,
    case when birthdate_is_valid then birthdate_raw end as birthdate,
    birthdate_is_valid,
    trim(split_part(address_raw, '|', 1)) as street,
    nullif(trim(regexp_extract(trim(split_part(address_raw, '|', 2)), '^(.*?),?\s*([A-Z]{2})\s+(\d{5})$', 1)), '') as city,
    nullif(regexp_extract(trim(split_part(address_raw, '|', 2)), '^(.*?),?\s*([A-Z]{2})\s+(\d{5})$', 2), '') as state,
    nullif(regexp_extract(trim(split_part(address_raw, '|', 2)), '^(.*?),?\s*([A-Z]{2})\s+(\d{5})$', 3), '') as zip
from checked
