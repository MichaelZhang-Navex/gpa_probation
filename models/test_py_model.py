import os




def model(dbt, session):
    dbt.config(enabled = False)
    my_sql_model_df = dbt.ref("course_type").df()
    my_sql_model_df = my_sql_model_df[my_sql_model_df['course_type']!="PASS_FAIL"]
    print(os.getcwd())
    return my_sql_model_df