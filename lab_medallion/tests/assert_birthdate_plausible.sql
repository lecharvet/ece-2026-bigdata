{{ config(severity='warn') }}
-- warn seulement : date de naissance impossible (future ou avant 1900)
select * from {{ ref('stg_users') }}
where birthdate > current_date or birthdate < date '1900-01-01'
