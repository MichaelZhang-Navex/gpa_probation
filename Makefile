id ?="[1500964]" # Default list of student IDs

import_excel:
	dbt run -s raw_import_result

build:
	dbt seed 
	dbt run -s tag:build

roll_one:
	dbt run -q --no-use-colors --vars '{"student_id": '"$$id"'}' -s roll_one_gpa > roll_one_result.log

roll_all:
	dbt run -s roll_gpa