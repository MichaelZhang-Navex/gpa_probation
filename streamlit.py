import streamlit as st
import duckdb
import pandas as pd

# Connect to the DuckDB database
con = duckdb.connect(database='database.duckdb', read_only=True)

# Streamlit app
st.title("Student GPA Lookup")

# Button to clear search history
if st.button("Clear Search History"):
    st.session_state.search_history = []
    st.success("Search history cleared.")

# Text area for multiple student IDs
student_ids = st.text_area("Enter 7-digit student IDs, one per line:")

# Initialize session state to keep search history
if 'search_history' not in st.session_state:
    st.session_state.search_history = []

# Process each student ID
if student_ids:
    ids = student_ids.splitlines()
    for student_id in ids:
        student_id = student_id.strip()
        if student_id.isdigit() and len(student_id) == 7:
            # Run the query
            query = f"SELECT * FROM gold_rolled_gpa WHERE student_id = {student_id}"
            try:
                # Fetch the result
                result = con.execute(query).df()

                # Check if the result is empty
                if result.empty:
                    st.warning(f"No record found for student ID {student_id}.")
                else:
                    # Format the student_id as a digit without thousand comma
                    result['student_id'] = result['student_id'].apply(lambda x: f"{int(x):d}")
                    result['term'] = result['term'].apply(lambda x: f"{int(x):d}")
                    
                    # Add the result to search history
                    st.session_state.search_history.append(result)
                    
                    # Display the result
                    # st.write("Student Result:")
                    # st.dataframe(result, use_container_width=True)
            except Exception as e:
                st.error(f"An error occurred with student ID {student_id}: {e}")
        else:
            st.error(f"Please enter a valid 7-digit student ID: {student_id}")

# Display all search history
if st.session_state.search_history:
    st.write("Search History:")
    for idx, df in enumerate(st.session_state.search_history):
        st.write(f"Search {idx + 1}:")
        st.dataframe(df, use_container_width=True)

# Close the DuckDB connection
con.close()
