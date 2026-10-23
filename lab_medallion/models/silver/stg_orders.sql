with src as (
    select * from {{ source('bronze', 'orders') }}
),

cleaned as (
    select
        trim(uuid)                       as order_id,
        trim(user_uuid)                  as user_id,
        try_cast(date as timestamptz)    as ordered_at,
        try_cast(quantity as integer)    as quantity,
        lower(trim(product))             as product
    from src
    where uuid is not null
)

select *
from cleaned
-- dedoublonnage : on garde la ligne la plus recente par commande
qualify row_number() over (partition by order_id order by ordered_at desc) = 1
