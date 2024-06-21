{{
  config(
    materialized = 'table',
    )
}}

WITH full_subject AS (
    SELECT
        DISTINCT course_name, subject_catalog
    FROM
        {{ ref('raw_gpa_related_table') }}
),
FINAL AS (
    SELECT
        fs.course_name course_name,
        fs.subject_catalog subject_catalog,
        CASE
            -- WHEN pf.subject_catalog IS NOT NULL THEN 'PASS_FAIL'   -- priority
            WHEN ar.subject_catalog IS NOT NULL THEN 'ALLOW_REPEAT'
            ELSE 'REGULAR'
        END AS course_type
    FROM
        full_subject fs
        LEFT JOIN {{ ref('seed_allow_repeat') }} ar USING (subject_catalog)
        LEFT JOIN {{ ref('seed_pass_fail_class') }} pf USING (subject_catalog)
)
SELECT
    *
FROM
    FINAL
