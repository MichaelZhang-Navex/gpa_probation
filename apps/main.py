#%%

import duckdb

"""
select * from silver_gpa_table
where course_name = 'HIST-215-12';

select id, subject_catalog, count(*) as ct
from silver_gpa_table
group by 1,2
having ct >8
order by 1,2;




                  select distinct
                    id,
                    term,
                    course_name,
                    subject_catalog,
                    grade,
                    unit_taken,
                    grd_pt_per_unit,
                    grade_points,
                    enrl_tot_gpa,
                    course_type
                   from silver_gpa_table
                  where 
                  id = 3036577 
                  -- course_type = 'PASS_FAIL'
                    -- and grade = 'F'
                  -- subject_catalog = 'ECED-455'
                  order by id, term desc;




                  with cte as (
                  select
                    id,
                    term,
                    course_name,
                  subject_catalog,
                    grade,
                    unit_taken,
                    grd_pt_per_unit,
                    grade_points,
                    enrl_tot_gpa,
                    course_type
                from silver_gpa_table
                where 
                   subject_catalog = 'MUSA-391'
                  and id = 1411506
                order by id, term desc)
                  select * from cte


                  select * from silver_gpa_table limit 10
"""

#%%
import duckdb

with duckdb.connect("../database.duckdb") as con:
    res = con.sql("""
select 
    id,
    term,
    course_name as full_subject_id,
    grade,
    unit_taken,
    grd_pt_per_unit,
    grd_points,
    enrl_tot_gpa,
    grade_points,
    course_type
from silver_gpa_table
where full_subject_id like 'HIST-103%'
            ;""").to_df()
    # res.to_excel('output.excel', index=False)
    res.to_excel('output.xlsx', index=False)
    # print(res)



#%%
