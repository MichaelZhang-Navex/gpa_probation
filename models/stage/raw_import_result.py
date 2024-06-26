from glob import glob
import pandas as pd
import warnings
import os

warnings.filterwarnings("ignore", category=UserWarning)

COLUMNS = [
    ("id", "BIGINT"),
    ("term", "BIGINT"),
    ("grade", "VARCHAR"),
    ("grd_points", "DOUBLE"),
    ("class_nbr", "BIGINT"),
    ("session", "VARCHAR"),
    ("grd_pt_per_unit", "DOUBLE"),
    ("section", "VARCHAR"),
    ("subject", "VARCHAR"),
    ("catalog", "VARCHAR"),
    ("career", "VARCHAR"),
    ("unit_taken", "BIGINT"),
    ("sex", "VARCHAR"),
    ("last", "VARCHAR"),
    ("first_name", "VARCHAR"),
    ("postal", "VARCHAR"),
    ("prim_prog", "VARCHAR"),
    ("strt_level", "BIGINT"),
    ("term_attmpt", "BIGINT"),
    ("term_pass", "BIGINT"),
    ("su_attmpt", "BIGINT"),
    ("su_pass", "BIGINT"),
    ("cum_earn_total", "DOUBLE"),
    ("term_gpa", "DOUBLE"),
    ("cum_gpa", "DOUBLE"),
    ("enrl_cum_gpa", "DOUBLE"),
    ("grade_points", "DOUBLE"),
    ("enrl_tot_gpa", "DOUBLE"),
    ("topic_id", "DOUBLE"),
    ("repeat", "VARCHAR"),
]


def load_file(file_path: str) -> pd.DataFrame:

    data = pd.read_excel(file_path, skiprows=1)
    data.columns = [i[0] for i in COLUMNS]
    return data


def model(dbt, session):

    dbt.config(tags=["import"])
    dbt.config(materialized="table")

    _DATA_PATH = dbt.config.get("data_location")

    file_list = glob(os.path.join(_DATA_PATH, "*.xlsx"))

    print(f"files to import: \n {file_list}")
    # return pd.DataFrame({'id': [1,2,3]})

    session.sql(
        f"""create or replace table all_gpa_table ( {', '.join([f"{i[0]} {i[1]}" for i in COLUMNS])} );""".strip()
    )

    import_result = []
    for key, file in enumerate(file_list):
        try:
            dd = load_file(file)
            session.sql("insert into all_gpa_table  SELECT * FROM dd")
            print(f"{file} imported")
            result = "success"
        except Exception as err:
            result = err
        print([key, file, result])
        import_result.append([key, file, result])

    result = pd.DataFrame(import_result, columns=["id", "file", "result"])
    print(result)
    return result
