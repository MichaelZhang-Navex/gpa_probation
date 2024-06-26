{{ config(
    materialized = 'table',
    tags = ['build'],
) }}

SELECT
    *
FROM
    {{ ref('raw_gpa_related_table') }}
