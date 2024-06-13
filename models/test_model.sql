{{
  config(
    materialized = 'table',
    alias = 'some_table_name'
    )
}}

SELECT
    *
FROM
    {{ source(
        'main',
        'all_gpa_table'
    ) }}
LIMIT
    5
