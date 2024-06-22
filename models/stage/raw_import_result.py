from glob import glob
import pandas as pd
import warnings
import os

warnings.filterwarnings("ignore", category=UserWarning)


def load_file(file_path: str) -> pd.DataFrame:
    columns = [
        "id",
        "term",
        "grade",
        "grd_points",
        "class_nbr",
        "session",
        "grd_pt_per_unit",
        "section",
        "subject",
        "catalog",
        "career",
        "unit_taken",
        "sex",
        "last",
        "first_name",
        "postal",
        "prim_prog",
        "strt_level",
        "term_attmpt",
        "term_pass",
        "su_attmpt",
        "su_pass",
        "cum_earn_total",
        "term_gpa",
        "cum_gpa",
        "enrl_cum_gpa",
        "grade_points",
        "enrl_tot_gpa",
        "topic_id",
        "repeat",
    ]
    data = pd.read_excel(file_path, skiprows=1)
    data.columns = columns
    return data


def model(dbt, session):

    dbt.config(tags=["load"])
    dbt.config(materialized="table")

    _DATA_PATH = dbt.config.get("data_location")
    print(_DATA_PATH)

    file_list = glob(os.path.join(_DATA_PATH, "*.xlsx"))

    print(file_list)
    # return pd.DataFrame({'id': [1,2,3]})

    session.sql(
        """
        create or replace table all_gpa_table(
        id               BIGINT,
        term             BIGINT,
        grade            VARCHAR,
        grd_points       BIGINT,
        class_nbr        BIGINT,
        session          VARCHAR,
        grd_pt_per_unit  BIGINT,
        section          VARCHAR,
        subject          VARCHAR,
        catalog          VARCHAR,
        career           VARCHAR,
        unit_taken       BIGINT,
        sex              VARCHAR,
        last             VARCHAR,
        first_name       VARCHAR,
        postal           VARCHAR,
        prim_prog        VARCHAR,
        strt_level       BIGINT,
        term_attmpt      BIGINT,
        term_pass        BIGINT,
        su_attmpt        BIGINT,
        su_pass          BIGINT,
        cum_earn_total   DOUBLE,
        term_gpa         DOUBLE,
        cum_gpa          DOUBLE,
        enrl_cum_gpa     DOUBLE,
        grade_points     DOUBLE,
        enrl_tot_gpa     DOUBLE,
        topic_id         DOUBLE,
        repeat           VARCHAR
        );
    """
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
