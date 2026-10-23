-- username_normalized doit etre trim + minuscules
select * from {{ ref('stg_users') }}
where username_normalized <> lower(trim(username_normalized))
