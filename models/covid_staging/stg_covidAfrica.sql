{{ config(materialized = 'table') }}

SELECT *  FROM {{ source('covid_africa', 'stg_covidAfrica') }}