INSTALL spatial;
LOAD spatial;

copy (
WITH student_cte AS (
SELECT
	student_id AS id
FROM
	gold_rolled_gpa
WHERE
	term = 0
	AND (total_gpa <> 0
		OR grade_points <> 0)
	AND (total_gpa + grade_points < 20 )
	AND (total_gpa + grade_points > -20 )
)
SELECT g.* FROM silver_gpa_table g INNER JOIN student_cte s USING (id) 
ORDER BY id, term desc
) TO "data_dump/all_students_with_error .xlsx" WITH (FORMAT GDAL, DRIVER 'xlsx');


copy (
WITH student_cte AS (
SELECT
	student_id
FROM
	gold_rolled_gpa
WHERE
	term = 0
	AND (total_gpa <> 0
		OR grade_points <> 0)
	AND (total_gpa + grade_points < 20 )
	AND (total_gpa + grade_points > -20 )
)
SELECT g.* FROM gold_rolled_gpa g INNER JOIN student_cte s USING (student_id)
ORDER BY student_id, term desc
) TO "data_dump/all_rolled_with_error.xlsx" WITH (FORMAT GDAL, DRIVER 'xlsx');
