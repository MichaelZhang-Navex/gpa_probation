{{ config(
    materialized = 'table',
) }}

SELECT
    *
FROM
    {{ ref('raw_gpa_related_table') }}
    rgrt
    LEFT JOIN {{ ref('raw_course_type') }}
    rct USING (course_name)
