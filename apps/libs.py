from typing import Tuple
import pandas as pd
import math
import warnings

warnings.simplefilter(action='ignore', category=Warning)

class GPARollBack:
    student_df: pd.DataFrame
    df: pd.DataFrame
    student_id: int
    terms: list
    subjects: list
    by_terms: dict
    by_subjects: dict

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
        "course_type",
    ]

    _picked_columns = [
        "term",  # 1
        "full_subject_id",
        "grade",
        "unit_taken",
        "total_gpa",
        "grade_points_per_unit",
        "grade_points",
        "course_type",
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

        course_type = self.by_subjects[subject_name]["current"]["course_type"].iloc[0]
        current_row = self.by_subjects[subject_name]["current"].iloc[0:1, :]

        #  remove current row
        self.by_subjects[subject_name]["current"] = self.by_subjects[subject_name][
            "current"
        ].iloc[1:, :]

        # append to past row
        self.by_subjects[subject_name]["past"] = pd.concat(
            [self.by_subjects[subject_name]["past"], current_row]
        )

        # pass fail
        if current_row["grade"].iloc[0] == "PS":
            minus_value = (0, 0)
        else:
            minus_value = (
                current_row["unit_taken"].iloc[0],
                current_row["grade_points_per_unit"].iloc[0],
            )

        previous_row = self.by_subjects[subject_name]["current"].iloc[0:1, :]

        # allow repeat
        if course_type == "ALLOW_REPEAT":
            plus_value = (0, 0)

        # regular
        else:
            plus_value = (
                (
                    previous_row["unit_taken"].iloc[0],
                    previous_row["grade_points_per_unit"].iloc[0],
                )
                if len(self.by_subjects[subject_name]["current"]) > 0
                else (0, 0)
            )
        # print((subject_name, minus_value, plus_value))
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

        gpa_by_term = [current_gpa]

        #  Initialize value. for total_gpa and grade_points,
        #  only the current term ha accurate data.
        # {'term': 2178, 'total_gpa': 112, 'grade_points': 214}

        # iterate over term + course.
        # unit taken: minus [unit_taken]
        # grade_points_per_unit, minus [unit_taken] * [grade_points_per_unit]

        for term_key, term in enumerate(self.terms):

            term_df = self.by_terms[term]["current"]
            for _, row in term_df.iterrows():

                subject = row["full_subject_id"]

                roll_repeat_subject = self._pick_repeat_shift_value(subject)
                (subject, (units_minus, points_minus), (units_plus, points_plus)) = (
                    roll_repeat_subject
                )

                total_gpa = current_gpa["total_gpa"] - units_minus + units_plus

                if not total_gpa >= 0: print(f"{id} got a negative total_gpa: {total_gpa}")

                grade_points = (
                    current_gpa["grade_points"]
                    - (units_minus * points_minus)
                    + (units_plus * points_plus)
                )

                current_gpa = {
                    "term": (
                        self.terms[term_key + 1]
                        if term_key < len(self.terms) - 1
                        else 0
                    ),
                    "total_gpa": total_gpa,
                    "grade_points": grade_points,
                }
                pass
                # print(roll_repeat_subject)
            gpa_by_term.append(current_gpa)
        # print(json.dumps(gpa_by_term))
        gpa_df = pd.DataFrame(gpa_by_term)
        gpa = round(gpa_df["grade_points"] / gpa_df["total_gpa"], 3)

        if not all(
            (0 <= g <= 4) or math.isnan(g) for g in gpa
        ): print(f"{id}'s GPA is out of range: {gpa}")
        gpa_df["GPA"] = gpa
        self.rolled_df = gpa_df
        return self


if __name__ == "__main__":

    import duckdb

    con = duckdb.connect("database.duckdb")

    student_ids = con.sql(
        """
        select distinct id from silver_gpa_table order by id;
    """
    ).to_df()
    
    con.sql("truncate TABLE rolled_gpa")

    for id in student_ids["id"][0:100]:
        df = con.sql(
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
                course_type
            from silver_gpa_table
            where id = {id}
                ;"""
        ).to_df()

        student_roll_back = (
            GPARollBack(student_id=id, student_df=df).load_data().rollback().rolled_df
        )
        # print(student_roll_back)
        con.sql(f"insert into rolled_gpa select {id} as student_id, * from student_roll_back;")
    con.close()
