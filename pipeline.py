import sqlite3
import os
import json
import pandas as pd
import logging
import numpy as np
import datetime

os.chdir('/Users/ayokunle/Documents/mylab/codecademy_data_pipeline_proj/src')

logging.basicConfig(
    filename=f'./log/devpipeline_{datetime.datetime.today().strftime("%Y-%m-%d-%M-%s")}.log', 
    filemode='w', 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)

logger = logging.getLogger(__name__)


def clean_students_table(df):
  
    df['contact_info'] = df['contact_info'].map(lambda x: json.loads(x) if isinstance(x, str) else x )
    # explode 
    df_contact_info = pd.json_normalize(df['contact_info'])

    # join
    # reset index to prevent nans when concatenating along the columns
    df = pd.concat([df.reset_index(drop=True), df_contact_info.reset_index(drop=True)], axis='columns')
    df = df.drop(['contact_info'], axis='columns')

    # assuming df with no jobs have a no_job category
    # df['job_id'].unique()

    # fill the df dataframe with that id to maintain integrity: job_id 999, 
    df['job_id'] = df['job_id'].fillna(999)

    # df that have not started learning or signed up for any career path
    # i.e num_course_taken, current_career_path_id and time_spent_hrs is null 
    conditions = ( 
        (df.num_course_taken.isna()) &
        (df.current_career_path_id.isna()) &
        (df.time_spent_hrs).isna()
    )
    df.loc[conditions, :] = df.loc[conditions, :].fillna({'num_course_taken': 0, 'current_career_path_id': 999,'time_spent_hrs': 0})

    # structurally missing data where df havent selected career path so time spent is 0 
    df = df.fillna({'current_career_path_id': 999,'time_spent_hrs': 0})

    # rows with random missing data
    incomplete_data_df = df[df.isna().any(axis=1)]

    # final clean up
    df = df.dropna()

    # convert dob to datetime, 
    # num_course_taken, current_career_path_id,job_id to integer, 
    # time_spent_hrs to float

    df['dob'] = df['dob'].astype('datetime64[ns]')
    df['num_course_taken'] = df['num_course_taken'].astype(float)
    df['current_career_path_id'] = df['current_career_path_id'].astype(float).astype(int)
    df['job_id'] = df['job_id'].astype(float).astype(int)
    df['time_spent_hrs'] = df['time_spent_hrs'].astype(float)
    # incomplete_data_df = df.isna().any()
    return df, incomplete_data_df

def clean_career_paths_table(df):
    new_career_path = {'career_path_id': 999, 'career_path_name':	'undefined', 'hours_to_complete': 0}
    if (new_career_path['career_path_id'] not in df['career_path_id'].values):
        df.loc[len(df)] = new_career_path
    else:
        pass
    return df

def clean_student_jobs_table(df):
    new_job_category = {'job_id': 999, 'job_category': 'undefined', 'avg_salary': 0}
    if (new_job_category['job_id'] not in df['job_id'].values):
        df.loc[len(df)] = new_job_category
    else:
        pass
    df.loc[len(df)] = new_job_category

    # drop duplicates
    df = df.drop_duplicates().reset_index(drop=True)
    return df


### tests

# test for nulls
# test for equal rows in db and final csv
# test for secondary keys in 
# test for equal columns

def test_for_nulls(df):
    missing_df = df[df.isna().any(axis='columns')]
    count = len(missing_df)

    try:
        assert count == 0, f"{str(count)} rows are missing values"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    
def test_equal_rows(local_df, db_df):
    try:
        assert len(local_df) == len(db_df), "aggregated database table not consistent with aggregated dataframe"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae

def test_equal_columns(local_df, db_df):

    # for troubleshooting
    # print(f"local df cols: {str(local_df.columns)}")
    # print(f"\ndb df cols: {str(db_df.columns)}")

    try:
        assert_msg = "aggregated database table columsn not consistent with aggregated dataframe columns"
        assert local_df.columns.to_list() == db_df.columns.to_list(), assert_msg
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        logger.info("Function test_equal_columns called, passed succesfully")

def test_schema(local_df, db_df):

    # print(local_df.dtypes, db_df.dtypes)

    errors = 0
    for column in db_df.columns:
        try:
            if local_df[column].dtypes != db_df[column].dtypes:
                errors += 1
        except NameError as ne:
            raise ne
    
    try:
        assert errors == 0, f"There are {str(errors)} schema errors"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        logger.info("Function test_schema called, passed succesfully")
        
def test_job_id_foreign_keys(students_df, student_jobs_df):
    current_job_ids = set(students_df['job_id'].unique())
    all_job_ids = set(student_jobs_df['job_id'].unique())
    missing_ids = list(current_job_ids - all_job_ids)

    try:
        assert len(missing_ids) == 0, f"{str(list(missing_ids))} job ids are missing in the students_jobs dimension table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        logger.info("Function test_job_id_foreign_keys called, passed succesfully")

def test_career_id_foreign_keys(students_df, courses_df):
    all_course_ids = set(courses_df['career_path_id'].unique())
    course_ids = set(students_df['current_career_path_id'].unique())
    
    # is_subset = np.isin(students_df['current_career_path_id'].unique(), all_course_ids)
    missing_ids = (course_ids - all_course_ids)

    try:
        assert len(missing_ids) == 0, f"{str(list(missing_ids))} path ids are missing in the courses/career path dimension table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        logger.info("Function test_career_id_foreign_keys called, passed succesfully")


def main():

    logger.info("Starting log")
    # # Create connection object

    # connect to source db
    try:
        db_connection = sqlite3.connect('./dev/cademycode.db')
        # db_connection = sqlite3.connect('./dev/cademycode_updated.db')
        # Create cursor object
        cursor = db_connection.cursor()

        # tables = cursor.execute("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name;")
        # print(tables)

        courses = pd.read_sql('SELECT * FROM cademycode_courses', db_connection)
        student_jobs = pd.read_sql_query('SELECT * FROM cademycode_student_jobs', db_connection)
        students = pd.read_sql_query('SELECT * FROM cademycode_students', db_connection)

        cursor.close()
    except sqlite3.Error as error:
        print("Error while working with SQLite", error)
    finally:
        if db_connection:
            db_connection.close()

    # connect to prod db
    try:
        db_connection = sqlite3.connect('./prod/cademycode.db') # throw exception if db table does not exist, will be created later

        aggregated_students = pd.read_sql_query('SELECT * FROM cademycode_aggregated', db_connection, parse_dates=['dob'])
        # aggregated_missing_data = pd.read_sql_query('SELECT * FROM cademycode_aggregated', db_connection)

        # get the newly added data
        new_students = students[~ np.isin(students['uuid'].unique(), aggregated_students['uuid'].unique())]
        print("before calliing clean_students_table function")
        print(new_students.shape)
    except: # catch any exception e.g if prod db does not exist
        new_students = students
        aggregated_students = []
    finally:
        db_connection.close()

    clean_new_students, new_missing_data = clean_students_table(new_students)

    print("after calliing clean_students_table function")
    print(clean_new_students.shape)

    # only clean dimension other tables if there are new students
    if len(clean_new_students) > 0:
        clean_career_paths = clean_career_paths_table(courses)
        clean_student_jobs = clean_student_jobs_table(student_jobs)

        # run tests for foreign keys before merging
        test_career_id_foreign_keys(clean_new_students, clean_career_paths)
        test_job_id_foreign_keys(clean_new_students, clean_student_jobs)

        # merge new data using the clean dimension table
        new_aggregated_students = pd.merge(
            clean_new_students, 
            clean_career_paths,
            how='inner',
            left_on='current_career_path_id', right_on='career_path_id'
        )

        new_aggregated_students = pd.merge(
            new_aggregated_students,
            clean_student_jobs,
            how='inner',
            on='job_id'
        )
        
        # if there was already data in prod
        # verify prod schema/columns and new data schema/columns
        if len(aggregated_students) > 0:
            test_schema(new_aggregated_students, aggregated_students)
            test_equal_columns(new_aggregated_students, aggregated_students)

        # check for nulls
        test_for_nulls(new_aggregated_students)

        # upsert
        db_connection = sqlite3.connect('./prod/cademycode.db')
        new_aggregated_students.to_sql('cademycode_aggregated', db_connection, if_exists='append', index=False)

        # sanity check 
        total_aggregated_students = pd.read_sql("SELECT * FROM cademycode_aggregated", db_connection)

        # save to file 
        total_aggregated_students.to_csv('./prod/aggreagated_students.csv')

        log_msg = f"added {str(len(new_aggregated_students))} rows to table"
        logger.info(log_msg)

    
    else:
        logger.info("no new data")

    logger.info("End log")


if __name__ == "__main__":
    main()