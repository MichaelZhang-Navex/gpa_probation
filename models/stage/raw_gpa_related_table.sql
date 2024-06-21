{{ config(
    materialized = 'view'
) }}

WITH update_comm_cte AS (

    SELECT
        id,
        term,
        grade,
        grd_points,
        -- class_nbr,
        grd_pt_per_unit,
        CASE
            WHEN subject = 'CMAT' THEN 'COMM'
            ELSE subject
        END AS subject,
        -- CMAT 改成 COMM
        "catalog",
        unit_taken,
        -- prim_prog,
        grade_points,
        enrl_tot_gpa,
        topic_id::int::text as topic_id
    FROM
        {{ source(
            'main',
            'all_gpa_table'
        ) }}
    WHERE
        prim_prog <> 'UGND'
        AND grade IN (
            'A',
            'B',
            'C',
            'D',
            'F',
            'PS'
        )
)
SELECT
    id,
    term,
    "subject" || '-' || "catalog"  || '-' || "topic_id" AS course_name,
    "subject" || '-' || "catalog"  as subject_catalog,
    grade,
    unit_taken,
    grd_pt_per_unit,
    grd_points,
    grade_points,
    enrl_tot_gpa
FROM
    update_comm_cte
