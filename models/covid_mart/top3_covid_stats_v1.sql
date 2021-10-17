{{ config(materialized = 'table') }}


WITH best_three_countries as (
SELECT * 
FROM {{ ref('countries_with_si_v1') }} 
ORDER by severity_index asc 
LIMIT 3)

SELECT 
best_three_con.country
, ROUND(best_three_con.Total_Recovered/stats.total_recovered, 5) recovery_ratio
, ROUND(best_three_con.Total_Deaths / stats.median, 5) death_ratio
, ROUND(best_three_con.Total_Tests/stats.std_deviation, 5) tests_ratio
from  best_three_countries best_three_con
INNER JOIN {{ ref('covid_africa_stats_v1') }} stats on 1=1