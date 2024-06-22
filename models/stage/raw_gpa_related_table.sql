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
        topic_id::int::text as topic_id,
        repeat
    FROM
        {{ source(
            'main',
            'all_gpa_table'
        ) }}
    WHERE
        -- prim_prog <> 'UGND' AND
        grade IN (
            'A',
            'B',
            'C',
            'D',
            'F',
            'PS'
        )
),
original_catalog as(
SELECT
    id,
    term,
    {# "subject" || '-' || "catalog"  || '-' || "topic_id" AS course_name, #}
    "subject" || '-' || "catalog"  as subject_catalog,
    grade,
    unit_taken,
    grd_pt_per_unit,
    grd_points,
    grade_points,
    enrl_tot_gpa,
    topic_id,
    repeat
FROM
    update_comm_cte
)
,non_repeatable_catalog as (
SELECT
    cat.id,
    cat.term,
    -- cat.subject_catalog,
    coalesce(dup.course, cat.subject_catalog)  as course_name,
    cat.subject_catalog as original_course,
    dup.course as replaced_course,
    cat.grade,
    cat.unit_taken,
    cat.grd_pt_per_unit,
    cat.grd_points,
    cat.grade_points,
    cat.enrl_tot_gpa,
    cat.repeat
from original_catalog cat
left join {{ ref('no_duplicate_credits') }} dup
on cat.subject_catalog = dup.may_not_receive
)
select * from non_repeatable_catalog