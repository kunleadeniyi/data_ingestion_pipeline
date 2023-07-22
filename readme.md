# Data Pipeline Project
# Source: Codecademy

## Introduction

This project is designed to handle data ingestion, cleaning, and loading tasks from a source SQLite database to a target SQLite database for Codecademy's student data. The data pipeline includes several phases such as data extraction, cleaning, transformation, and loading into a production database.

## Dependencies

The project is implemented in Python and requires the following libraries:
- sqlite3
- os
- json
- pandas
- logging
- numpy
- datetime

## File Structure

The project has the following file structure:


- src/                               # Source code directory
    - data_pipeline.py               # Main script
- dev/                               # Development database directory
    - cademycode.db                  # Development SQLite database
- prod/                              # Production database directory
    - cademycode.db                  # Production SQLite database
- log/                               # Log files directory
    - devpipeline_{datetime}.log     # Log file
- Documents/                         
    - mylab/
        - codecademy_data_pipeline_proj/
            - src/
                - data_pipeline.py

## Project Structure

The project consists of several Python functions to handle different tasks in the data pipeline:

- `clean_students_table(df)`: Cleans the students' data by handling missing values, transforming data types, and flattening nested JSON columns.
- `clean_career_paths_table(df)`: Cleans the career paths data.
- `clean_student_jobs_table(df)`: Cleans the student jobs data.
- `test_for_nulls(df)`, `test_equal_rows(local_df, db_df)`, `test_equal_columns(local_df, db_df)`, `test_schema(local_df, db_df)`, `test_job_id_foreign_keys(students_df, student_jobs_df)`, `test_career_id_foreign_keys(students_df, courses_df)`: These functions serve as data validation checks.
- `main()`: The main function which orchestrates the data pipeline process.

## Usage

To execute the pipeline, run the Python script using a Python interpreter:

python data_pipeline.py
Note: You must be in the 'src' directory where the Python script resides.

## Logging
The script logs events into a log file located at ./log/devpipeline_{datetime}.log. It tracks various stages of the pipeline, errors, and exceptions that occurred during execution.

