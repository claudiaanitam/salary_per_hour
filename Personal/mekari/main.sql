-- Assuming the SQL will be run on GCP environment

-- PART A: Create a schema and load each CSV file to employees and timesheets tables.
-- Schema employees and load employees CSV file
CREATE TABLE employees (
    employe_id INTEGER
    , branch_id INTEGER
    , salary INTEGER
    , join_date DATE
    , resign_date DATE
);

LOAD DATA OVERWRITE employees
FROM FILES (
  format = 'CSV',
  uris = ['gs://{bucket_name}/{file_path}/employees.csv']);

-- Schema timesheets and load timesheets CSV file
CREATE TABLE timesheets (
    timesheet_id INTEGER
    , employee_id INTEGER
    , date DATE
    , checkin TIME
    , checkout TIME
);

LOAD DATA OVERWRITE timesheets
FROM FILES (
  format = 'CSV',
  uris = ['gs://{bucket_name}/{file_path}/timesheets.csv']);

-- Schema destination table, assuming the destination table name is fact_detail_salary
CREATE TABLE fact_detail_salary (
    year INTEGER
    , month INTEGER
    , branch_id INTEGER
    , total_employee INTEGER
    , total_salary INTEGER
    , salary_per_hour FLOAT64
    , load_dt TIMESTAMP
)

-- PART B: Write an SQL script that reads from employees and timesheets tables, transforms,
-- and loads the destination table.
-- The script is expected to run daily in full-snapshot mode, meaning that it will read the
-- whole table and then overwrite the result in the destination table. Note that you don’t
-- have to implement the scheduler, just the script that will be run by the scheduler.

DROP TABLE IF EXISTS fact_detail_salary_temp;
CREATE TABLE fact_detail_salary_temp AS 
WITH data_employee AS (
    SELECT 
        *
    FROM employees
), data_timesheet AS (
    SELECT 
        *
    FROM timesheets
), base AS (
    SELECT 
        em.employe_id AS employee_id
        , branch_id
        , salary
        , join_date
        , resign_date
        , timesheet_id
        , date
        , EXTRACT(YEAR FROM date) AS year
        , EXTRACT(MONTH FROM date) AS month
        , checkin
        , checkout
        , FLOOR(TIME_DIFF(checkout, checkin, HOUR)) AS hour_diff
    FROM employees em
    LEFT JOIN timesheets ts
    ON em.employe_id = ts.employee_id
), base_agg AS (
    SELECT 
        year
        , month
        , branch_id
        , salary
        , COUNT(DISTINCT employee_id) AS total_employee
        , SUM(hour_diff) AS total_hour
    FROM base
    WHERE hour_diff IS NOT NULL
    GROUP BY 1, 2, 3
), base_agg_2 AS (
    SELECT 
        year
        , month
        , branch_id
        , SUM(salary) AS total_salary
        , SUM(total_hour) AS total_hour
        , SUM(total_employee) AS total_employee
    FROM base_agg
    GROUP BY 1, 2, 3
), main AS (
    SELECT  
        year
        , month
        , branch_id
        , total_employee
        , total_salary
        , (total_salary / total_hour) AS salary_per_hour
        , TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 HOUR) AS load_dt
    FROM base_agg_2
)

SELECT *
FROM main;

UPDATE 
    fact_detail_salary AS main
SET 
    main.year = temp.year
    , main.month = temp.month
    , main.branch_id = temp.branch_id
    , main.total_employee = temp.total_employee
    , main.total_salary = temp.total_salary
    , main.salary_per_hour = temp.salary_per_hour
    , main.load_dt = temp.load_dt
FROM fact_detail_salary_temp AS temp
WHERE 
    main.year = temp.year
    AND main.month = temp.month
    AND main.branch_id = temp.branch_id
;

INSERT INTO fact_detail_salary
SELECT 
    year
    , month
    , branch_id
    , total_employee
    , total_salary
    , salary_per_hour
    , load_dt
FROM 
    fact_detail_salary_temp
WHERE
    year||month||branch_id NOT IN (
        SELECT
            year||month||branch_id
        FROM
            fact_detail_salary
    );