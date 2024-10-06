import asyncio
import pandas as pd
from typing import Tuple
import pandas as pd
import math
import json_tricks
import warnings
import duckdb
from datetime import datetime

warnings.simplefilter(action="ignore", category=Warning)


ROLLED_GPA_TABLE = 'gold_rolled_gpa'
STEP = 32

class GPARollBack:
    student_df: pd.DataFrame
    df: pd.DataFrame
    student_id: int
    terms: list
    subjects: list
    by_terms: dict
    by_subjects: dict

    rollback_error: dict

    _columns = [
        "id",
        "term",
        "grade",
        "total_gpa",
        "actual_unit_taken",
        "grade_points",
        "grade_points_per_unit",
        "subject",
        "catalog",
        "full_subject_id",
        "unit_taken",
        "repeat",
    ]

    _picked_columns = [
        "term",  # 1
        "full_subject_id",
        "grade",
        "unit_taken",
        "total_gpa",
        "grade_points_per_unit",
        "grade_points",
        "repeat",
    ]

    def __init__(self, student_id: int, student_df: pd.DataFrame) -> "GPARollBack":
        self.student_df = student_df
        self.student_id = student_id

    def load_data(self) -> "GPARollBack":
        df = self.student_df
        # df = pd.read_excel(self._file_path)
        # df.columns = self._columns
        df.sort_values("term", ascending=False, inplace=True)

        df = df[df["grade"].isin(["A", "B", "C", "D", "F", "PS", "NP", "WF", "WP"])]
        df = df[self._picked_columns]

        self.df = df

        terms = df["term"].sort_values(ascending=False).unique()
        subjects = df["full_subject_id"].sort_values(ascending=False).unique()

        self.terms = terms
        self.subjects = subjects

        row_by_terms = {}
        row_by_subjects = {}

        for term in terms:
            row_by_terms[term] = {
                "current": pd.DataFrame([], columns=self._columns),
                "past": pd.DataFrame([], columns=self._columns),
            }

            current_term_df = df[df["term"] == term]
            # transforamtion for allow_repeat and pass_fail
            row_by_terms[term]["current"] = current_term_df

        for subject in subjects:
            row_by_subjects[subject] = {
                "current": pd.DataFrame([], columns=self._columns),
                "past": pd.DataFrame([], columns=self._columns),
            }
            row_by_subjects[subject]["current"] = df[df["full_subject_id"] == subject]

        self.by_terms = row_by_terms
        self.by_subjects = row_by_subjects

        return self

    def _pick_repeat_shift_value(
        self, subject_name: str
    ) -> Tuple[str, Tuple[int, int], Tuple[int, int]]:

        assert len(self.by_subjects[subject_name]["current"]) > 0

        current_row = self.by_subjects[subject_name]["current"].iloc[0:1, :]

        #  remove current row
        self.by_subjects[subject_name]["current"] = self.by_subjects[subject_name][
            "current"
        ].iloc[1:, :]

        # append to past row
        self.by_subjects[subject_name]["past"] = pd.concat(
            [self.by_subjects[subject_name]["past"], current_row]
        )

        # pass_fail
        if current_row["grade"].iloc[0] == "PS":
            minus_value = (0, 0)
        elif (
            current_row["grade"].iloc[0] in ["NP", "WF", "WP"]
            and current_row["repeat"].iloc[0] != "EXCL"
        ):
            minus_value = (0, 0)
        else:
            minus_value = (
                float(current_row["unit_taken"].iloc[0]),
                float(current_row["grade_points_per_unit"].iloc[0]),
            )

        previous_row = self.by_subjects[subject_name]["current"].iloc[0:1, :]

        plus_value = (
            (
                float(previous_row["unit_taken"].iloc[0]),
                float(previous_row["grade_points_per_unit"].iloc[0]),
            )
            if (len(self.by_subjects[subject_name]["current"]) > 0)
            and (previous_row["repeat"].iloc[0] == "EXCL")
            and (previous_row["grade"].iloc[0] != "PS")
            else (0, 0)
        )
        # if subject_name in ["PSYC-306"]:
        #     print(current_row["grade"].iloc[0])
        #     print(subject_name, minus_value, plus_value)

        return (subject_name, minus_value, plus_value)

    def rollback(self) -> "GPARollBack":
        """
        term   full_subject_id     grade    unit_taken  total_gpa   grade_points_per_unit    grade_points
        2178   MATH-413            F        4           112         0                        214
        2176   GEOG-141            C        3           108         2                        214
        2172   ENGL-103            D        4           105         1                        208
        2172   FTWL-106            B        3           105         3                        208
        2168   MATH-471            F        3            98         0                        195
        """
        current_gpa = {
            "term": self.df.iloc[0]["term"],
            "total_gpa": self.df.iloc[0]["total_gpa"],
            "grade_points": self.df.iloc[0]["grade_points"],
            "GPA": round(
                self.df.iloc[0]["grade_points"] / self.df.iloc[0]["total_gpa"], 3
            ),
        }

        self.rollback_error = {"id": self.student_id, "terms": []}

        gpa_by_term = [current_gpa]

        #  Initialize value. for total_gpa and grade_points,
        #  only the current term ha accurate data.
        # {'term': 2178, 'total_gpa': 112, 'grade_points': 214}

        # iterate over term + course.
        # unit taken: minus [unit_taken]
        # grade_points_per_unit, minus [unit_taken] * [grade_points_per_unit]

        for term_key, term in enumerate(self.terms):
            error = {"term": term, "error": []}
            term_df = self.by_terms[term]["current"]
            for _, row in term_df.iterrows():

                subject = row["full_subject_id"]

                roll_repeat_subject = self._pick_repeat_shift_value(subject)
                (subject, (units_minus, points_minus), (units_plus, points_plus)) = (
                    roll_repeat_subject
                )

                total_gpa = current_gpa["total_gpa"] - units_minus + units_plus

                grade_points = (
                    current_gpa["grade_points"]
                    - (units_minus * points_minus)
                    + (units_plus * points_plus)
                )
                gpa = round(grade_points / total_gpa, 3)

                current_gpa = {
                    "term": (
                        self.terms[term_key + 1]
                        if term_key < len(self.terms) - 1
                        else 0
                    ),
                    "total_gpa": total_gpa,
                    "grade_points": grade_points,
                    "GPA": gpa,
                }

            if total_gpa < 0:
                err_msg = f"got a negative total_gpa: {total_gpa}"
                # print(err_msg)
                error["error"].append(err_msg)
                self.rollback_error["has_error"] = True

            if not ((0 <= gpa <= 4) or math.isnan(gpa)):
                error_msg = f"GPA is out of range: \n{gpa}"
                # print(error_msg)
                error["error"].append(error_msg)
                error["has_error"] = True

            if all([current_gpa["total_gpa"] == 0, current_gpa["grade_points"] == 0]):
                error["to_zero"] = True

            gpa_by_term.append(current_gpa)
            self.rollback_error["terms"].append(error)
        # print(json.dumps(gpa_by_term))
        gpa_df = pd.DataFrame(gpa_by_term)

        self.rolled_df = gpa_df

        return self


async def roll_student(session, student_id, percent):
    print(f"processing: {student_id} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, {percent}%")
    df = session.sql(
        f"""
        select
            id,
            term, 
            course_name as full_subject_id,  
            grade,  
            unit_taken, 
            grd_pt_per_unit as grade_points_per_unit, 
            enrl_tot_gpa as total_gpa,
            grade_points,
            repeat
        from silver_gpa_table
        where id = {student_id}
        order by term desc, repeat desc
        ;"""
    ).to_df()

    rb = GPARollBack(student_id=student_id, student_df=df).load_data().rollback()
    rb_df = rb.rolled_df
    insert_query = f"insert into {ROLLED_GPA_TABLE} select {student_id} as student_id, * from rb_df;"
    session.sql(insert_query)

    roll_log = json_tricks.dumps(rb.rollback_error)
    # print(f"finished, {id}, { round((key+1)/students *100, 4) }%")

    roll_log_df = pd.DataFrame({"id": [student_id], "log": [roll_log]})
    return roll_log_df


async def main(session):

    session.sql(f"""
        create or replace table {ROLLED_GPA_TABLE} (
            student_id   BIGINT,
            term         BIGINT,
            total_gpa    BIGINT,
            grade_points BIGINT,
            GPA          DOUBLE
            );
    """)

    student_ids = session.sql(
        """
        select distinct id from silver_gpa_table;
    """
    ).to_df()

    student_id_count= len(student_ids)
    tasks = [roll_student(session, row['id'], round((key+1)/student_id_count, 4)) for key, row in student_ids.iterrows()]

    step = 10

    results = []

    for i in range(0, len(tasks), step):
        group_result =  await asyncio.gather(*tasks[i : i + step])
        results.append(pd.concat(group_result))
    return pd.concat(results)


def model(dbt, session):
    
    dbt.config(alias="gold_roll_gpa_log")
    dbt.config(materialized="table")
    # Run the asynchronous function and get the combined results
    combined_results = asyncio.run(main(session))
    return combined_results

class MyDBT:
    def config(self, *args, **kwargs):
        print(args, kwargs)

if __name__ == '__main__':
    session = duckdb.connect('database.duckdb')
    dbt = MyDBT()
    res = model(dbt, session)
    print(res)
    session.close()