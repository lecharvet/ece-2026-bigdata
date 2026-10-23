{{ config(severity='warn') }}
-- warn : une commande ne peut pas preceder la naissance de l'utilisateur
select o.order_id, o.user_id, o.ordered_at, u.birthdate
from {{ ref('stg_orders') }} o
join {{ ref('stg_users') }} u using (user_id)
where u.birthdate > cast(o.ordered_at as date)
