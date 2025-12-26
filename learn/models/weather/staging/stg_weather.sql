select
    ID as weather_id,
    CITY,
    COUNTRY as country_name, -- We will use this to join
    TEMPERATURE,
    HUMIDITY
from {{ source('snowflake_data', 'WEATHER_DATA') }}