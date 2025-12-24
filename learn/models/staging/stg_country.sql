select
    COUNTRYID as country_id,
    COUNTRY as country_name,
    CAPITAL,
    POPULATION,
    REGION
from {{ source('snowflake_data', 'COUNTRY') }}