from typing import Tuple
import pandas as pd
import math
import json_tricks
import warnings

warnings.simplefilter(action="ignore", category=Warning)


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

        df = df[df["grade"].isin(["A", "B", "C", "D", "F", "PS"])]
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
        else:
            minus_value = (
                current_row["unit_taken"].iloc[0],
                current_row["grade_points_per_unit"].iloc[0],
            )

        previous_row = self.by_subjects[subject_name]["current"].iloc[0:1, :]

        plus_value = (
            (
                previous_row["unit_taken"].iloc[0],
                previous_row["grade_points_per_unit"].iloc[0],
            )
            if (len(self.by_subjects[subject_name]["current"]) > 0)
            and (previous_row["repeat"].iloc[0] == "EXCL")
            else (0, 0)
        )
        # if subject_name == "COMM-101-0":
        #     print(self.by_subjects[subject_name]['current'])
        #     print(self.by_subjects[subject_name]['past'])
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
                print(err_msg)
                error["error"].append(err_msg)

            if not ((0 <= gpa <= 4) or math.isnan(gpa)):
                error_msg = f"GPA is out of range: \n{gpa}"
                print(error_msg)
                error["error"].append(error_msg)

            if all([current_gpa["total_gpa"] == 0, current_gpa["grade_points"] == 0]):
                error["to_zero"] = True

            gpa_by_term.append(current_gpa)
            self.rollback_error["terms"].append(error)
        # print(json.dumps(gpa_by_term))
        gpa_df = pd.DataFrame(gpa_by_term)

        self.rolled_df = gpa_df

        return self


if __name__ == "__main__":

    import duckdb

    con = duckdb.connect("database.duckdb")

    student_ids = con.sql(
        """
        select distinct id from silver_gpa_table order by id limit 100;
    """
    ).to_df()


    con.sql("truncate TABLE rolled_gpa;")
    roll_log = []
    for id in student_ids["id"]:
        df = con.sql(
            f"""
            select distinct 
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
            where id = {id}
            ;"""
        ).to_df()

        rb = GPARollBack(student_id=id, student_df=df).load_data().rollback()
        rb_df = rb.rolled_df
        con.sql(f"insert into rolled_gpa select {id} as student_id, * from rb_df;")

        roll_log.append(json_tricks.dumps(rb.rollback_error))

    student_ids['log'] = roll_log
    con.sql("create or replace table rollback_run_log as select * from student_ids;")

    con.close()

    # SELECT log->'$.id' FROM rollback_run_log;