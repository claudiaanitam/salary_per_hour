import numpy as np
import pandas as pd
from google.cloud import bigquery

# Write a Python or Java code that reads from CSV files, transforms, and loads to the
# destination table.
# The code is expected to run daily in incremental mode, meaning that each day it will only
# read the new data and then appends the result to the destination table. Note that you
# don’t have to implement the scheduler, just the script that will be run by the scheduler.

table_id_temp = "fact_detail_salary_temp"
file_path_employees = "https://github.com/claudiaanitam/salary_per_hour/blob/0cc9162a274d596d3991d473ffba00e04c1c215e/Personal/mekari/employees.csv"
file_path_timesheets = "https://github.com/claudiaanitam/salary_per_hour/blob/0cc9162a274d596d3991d473ffba00e04c1c215e/Personal/mekari/timesheets.csv"

def read_file_csv(file_path):
    """
        Function read file csv to pandas dataframe.
    """

    df = pd.read_csv(file_path)
    
    return df

def process_data():
    """
        All process including cleaning, transforming, and manipulating from raw data to final data.
    """    

    df_employees = read_file_csv(file_path_employees)

    # rename employe_id to employee_id, so we can combine the files
    df_employees = df_employees.rename(columns={"employe_id": "employee_id"})
    
    df_timesheets = read_file_csv(file_path_timesheets)

    # join two csv into one dataframe
    df = df_employees.merge(df_timesheets, on='employee_id', how='left')

    # define data type for each column
    dtypes_schema = {
        'employee_id': 'int64'
        , 'branch_id': 'int64'
        , 'salary': 'int64'
        , 'join_date': 'datetime64'
        , 'resign_date': 'datetime64'
        , 'timesheet_id': 'string'
        , 'date': 'datetime64'
        , 'checkin': 'string'
        , 'checkout': 'string'
    }

    for col in df.columns.tolist():
        df[col] = df[col].astype(dtypes_schema[col], errors="ignore")

    # extract date column to get year and month
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    # find how many hours the employee work on each day
    # since we calculate the salary based on hour, the number will be floor
    df["hour_diff"] = (((pd.to_datetime(df.checkout) - pd.to_datetime(df.checkin)).dt.total_seconds() / (60 * 60)).apply(np.floor))

    # take out the null data, since we can't calculate if one of the checkin/checkout column is null
    df = df[~df['hour_diff'].isnull()]

    # group by data to get total hours and total count of employee based on year, month, branch, and salary
    df2 = df
    df2 = df2.groupby(['year', 'month', 'branch_id', 'salary'], as_index=False).agg({"hour_diff": np.sum, "employee_id": pd.Series.nunique}).rename(columns={'hour_diff':'total_hour', 'employee_id': 'total_employee'})

    # group by data again to get total salary, total hour, and total employee based on year, month, and branch
    df2 = df2.groupby(['year', 'month', 'branch_id'], as_index=False).agg({"salary": np.sum, 'total_hour': np.sum, 'total_employee': np.sum}).rename(columns={'salary':'total_salary'})

    # calculate salary per hour by total salary and total hour
    df2['salary_per_hour'] = df2.total_salary / df2.total_hour

    # select column based on the needs
    df_result = df2[
        [
            'year'
            , 'month'
            , 'branch_id'
            , 'total_employee'
            , 'total_salary'
            , 'salary_per_hour'
        ]
    ]

    # clean and define data type again for each column to get a cleaner data
    df_result = np.round(df_result, decimals=2)

    dtypes_schema = {
        'year': 'int64'
        , 'month': 'int64'
        , 'branch_id': 'int64'
        , 'total_employee': 'int64'
        , 'total_salary': 'int64'
        , 'salary_per_hour': 'float64'
    }

    for col in df_result.columns.tolist():
        df_result[col] = df_result[col].astype(dtypes_schema[col], errors="ignore")

    print(df_result.head(5))

    return df_result

def load_to_dwh(write_mode, project_id, df, table_id):
    """
        Assuming it's on GCP environment and want to load the data to database Big Query.
    """

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode
    )

    client = bigquery.Client(project=project_id)
    client.load_table_from_dataframe(df, table_id, job_config=job_config)

    return None

if __name__ == '__main__':
    df_all = process_data()
    load_to_dwh("WRITE_APPEND", project_id, df_all, table_id_temp)
    