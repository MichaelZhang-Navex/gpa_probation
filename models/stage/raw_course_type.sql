{{
  config(
    materialized = 'table',
    )
}}

WITH full_subject AS (
    SELECT
        DISTINCT course_name
    FROM
        {{ ref('raw_gpa_related_table') }}
),
FINAL AS (
    SELECT
        fs.course_name course_name,
        CASE
            WHEN pf.course_name IS NOT NULL THEN 'PASS_FAIL'   -- priority
            WHEN ar.course_name IS NOT NULL THEN 'ALLOW_REPEAT'
            ELSE 'REGULAR'
        END AS course_type
    FROM
        full_subject fs
        LEFT JOIN main.seed_allow_repeat ar USING (course_name)
        LEFT JOIN main.seed_pass_fail_class pf USING (course_name)
)
SELECT
    *
FROM
    FINAL
