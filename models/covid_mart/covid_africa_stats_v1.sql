{{ config(materialized = 'table') }}

WITH covid_africa_stats as (
SELECT 
SUM(Total_Recovered) total_recovered,
(SELECT Max(median) FROM (SELECT PERCENTILE_CONT(Total_Deaths, 0.5) OVER() AS median FROM {{ ref('stg_covidAfrica') }} )) as median,
STDDEV(Total_Tests) std_deviation
FROM {{ ref('stg_covidAfrica') }} 
)

select * from covid_africa_stats