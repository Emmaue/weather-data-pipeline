{{ config(materialized='table') }}

with country_info as (
    select * from {{ ref('stg_country') }}
),

weather_info as (
    select * from {{ ref('stg_weather') }}
)

select
    -- 1. Create a Surrogate Key (Unique Hash)
    -- We combine the unique IDs from both sides to make a new unique key
    md5(cast(w.weather_id as varchar) || '-' || cast(c.country_id as varchar)) as unique_key,

    -- 2. Select columns from Weather
    w.weather_id,
    w.city,
    w.temperature,
    w.humidity,

    -- 3. Select columns from Country
    c.country_name,
    c.capital,
    c.population,
    c.region

from weather_info w
-- Join on the Country Name
left join country_info c
    on w.country_name = c.country_name