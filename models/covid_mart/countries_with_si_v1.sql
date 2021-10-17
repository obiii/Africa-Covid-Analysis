{{ config(materialized = 'table') }}

WITH ranking as (SELECT
country
, RANK() OVER ( ORDER BY Deaths_1_mil_population_ asc ) as deaths_1_mil_rank
, RANK() OVER ( ORDER BY Total_Cases_1_mil_population asc ) as total_cases_1_mil_rank
FROM {{ ref('stg_covidAfrica') }} 
)

, countries_with_si as (SELECT 
stg_all_countries.*
, rn.deaths_1_mil_rank
, rn.total_cases_1_mil_rank
, (deaths_1_mil_rank * total_cases_1_mil_rank) as severity_index
FROM {{ ref('stg_covidAfrica') }}  stg_all_countries 
INNER JOIN ranking  rn on rn.country = stg_all_countries.country
)

SELECT * FROM countries_with_si