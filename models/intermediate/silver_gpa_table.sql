{{ config(
    materialized = 'table',
) }}

SELECT
    *
FROM
    {{ ref('raw_gpa_related_table') }}
