-- ======================================
-- QUALITY CHECKS FOR silver.hr_demographics
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for required fields
SELECT 
    'employee_id' AS column_name, COUNT(*) AS null_count 
FROM silver.hr_demographics 
WHERE employee_id IS NULL
UNION ALL
SELECT 
    'gender' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_demographics 
WHERE gender IS NULL
UNION ALL
SELECT 
    'birthdate' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_demographics 
WHERE birthdate IS NULL
UNION ALL
SELECT 
    'position_id' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_demographics 
WHERE position_id IS NULL
UNION ALL
SELECT 
    'p_np' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_demographics 
WHERE p_np IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_demographics 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'employment_status' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_demographics 
WHERE employment_status IS NULL
UNION ALL
SELECT 
    'date_created' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_demographics 
WHERE date_created IS NULL
UNION ALL
SELECT 
    'date_updated' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_demographics 
WHERE date_updated IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    employee_id, 
    COUNT(*) AS duplicate_count
FROM silver.hr_demographics
GROUP BY employee_id
HAVING COUNT(*) > 1;

-- Check for unwanted whitespaces in string columns
-- Expectation: No Results
SELECT 
    'employee_id' AS column_name, 
    employee_id AS value
FROM silver.hr_demographics
WHERE employee_id != TRIM(employee_id)
UNION ALL
SELECT 
    'gender' AS column_name, 
    gender AS value
FROM silver.hr_demographics
WHERE gender != TRIM(gender)
UNION ALL
SELECT 
    'position_id' AS column_name, 
    position_id AS value
FROM silver.hr_demographics
WHERE position_id != TRIM(position_id)
UNION ALL
SELECT 
    'p_np' AS column_name, 
    p_np AS value
FROM silver.hr_demographics
WHERE p_np != TRIM(p_np)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.hr_demographics
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'employment_status' AS column_name, 
    employment_status AS value
FROM silver.hr_demographics
WHERE employment_status != TRIM(employment_status);

-- Check for valid gender values
-- Expectation: Review Results
SELECT DISTINCT 
    gender 
FROM silver.hr_demographics
WHERE gender NOT IN ('M', 'F', 'Male', 'Female', 'male', 'female', 'm', 'f');

-- Check for reasonable birth dates
-- Expectation: No Results for unreasonable dates
SELECT 
    employee_id,
    birthdate,
    EXTRACT(YEAR FROM birthdate) AS birth_year,
    EXTRACT(YEAR FROM AGE(birthdate)) AS current_age
FROM silver.hr_demographics
WHERE birthdate > CURRENT_DATE 
   OR birthdate < '1940-01-01'::DATE
   OR EXTRACT(YEAR FROM AGE(birthdate)) < 18
   OR EXTRACT(YEAR FROM AGE(birthdate)) > 80;

-- Check for referential integrity - company_id
-- Expectation: No Results
SELECT 
    DISTINCT company_id 
FROM silver.hr_demographics
WHERE company_id NOT IN (
    SELECT company_id 
    FROM ref.company_main
);

-- Check for referential integrity - position_id
-- Expectation: No Results
SELECT 
    DISTINCT position_id 
FROM silver.hr_demographics
WHERE position_id NOT IN (
    SELECT position_id 
    FROM ref.hr_position
);

-- Data standardization checks
SELECT DISTINCT 
    employment_status 
FROM silver.hr_demographics
ORDER BY employment_status;

SELECT DISTINCT 
    p_np 
FROM silver.hr_demographics
ORDER BY p_np;

-- ======================================
-- QUALITY CHECKS FOR silver.hr_parental_leave
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for required fields
SELECT 
    'parental_leave_id' AS column_name, COUNT(*) AS null_count 
FROM silver.hr_parental_leave 
WHERE parental_leave_id IS NULL
UNION ALL
SELECT 
    'employee_id' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_parental_leave 
WHERE employee_id IS NULL
UNION ALL
SELECT 
    'type_of_leave' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_parental_leave 
WHERE type_of_leave IS NULL
UNION ALL
SELECT 
    'date' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_parental_leave 
WHERE date IS NULL
UNION ALL
SELECT 
    'days' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_parental_leave 
WHERE days IS NULL
UNION ALL
SELECT 
    'end_date' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_parental_leave 
WHERE end_date IS NULL
UNION ALL
SELECT 
    'months_availed' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_parental_leave 
WHERE months_availed IS NULL
UNION ALL
SELECT 
    'date_created' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_parental_leave 
WHERE date_created IS NULL
UNION ALL
SELECT 
    'date_updated' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_parental_leave 
WHERE date_updated IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    parental_leave_id, 
    COUNT(*) AS duplicate_count
FROM silver.hr_parental_leave
GROUP BY parental_leave_id
HAVING COUNT(*) > 1;

-- Check for negative or zero days
-- Expectation: No Results
SELECT 
    parental_leave_id,
    employee_id,
    days
FROM silver.hr_parental_leave
WHERE days <= 0;

-- Check for reasonable days range (0-365)
-- Expectation: Review Results
SELECT 
    parental_leave_id,
    employee_id,
    days
FROM silver.hr_parental_leave
WHERE days > 365;

-- Check for referential integrity - employee_id
-- Expectation: No Results
SELECT 
    DISTINCT employee_id 
FROM silver.hr_parental_leave
WHERE employee_id NOT IN (
    SELECT employee_id 
    FROM silver.hr_demographics
);

-- Check for date consistency (end_date should be >= start date)
-- Expectation: No Results
SELECT 
    parental_leave_id,
    employee_id,
    date,
    end_date
FROM silver.hr_parental_leave
WHERE end_date < date;

-- Data standardization check
SELECT DISTINCT 
    type_of_leave 
FROM silver.hr_parental_leave
ORDER BY type_of_leave;

-- ======================================
-- QUALITY CHECKS FOR silver.hr_tenure
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for required fields
SELECT 
    'employee_id' AS column_name, COUNT(*) AS null_count 
FROM silver.hr_tenure 
WHERE employee_id IS NULL
UNION ALL
SELECT 
    'start_date' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_tenure 
WHERE start_date IS NULL
UNION ALL
SELECT 
    'end_date' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_tenure 
WHERE end_date IS NULL
UNION ALL
SELECT 
    'tenure_length' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_tenure 
WHERE tenure_length IS NULL
UNION ALL
SELECT 
    'date_created' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_tenure 
WHERE date_created IS NULL
UNION ALL
SELECT 
    'date_updated' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_tenure 
WHERE date_updated IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    start_date, 
    employee_id,
    COUNT(*) AS duplicate_count
FROM silver.hr_tenure
GROUP BY start_date, employee_id
HAVING COUNT(*) > 1;

-- Check for date consistency (end_date should be >= start_date)
-- Expectation: No Results
SELECT 
    employee_id,
    start_date,
    end_date
FROM silver.hr_tenure
WHERE end_date < start_date;

-- Check for negative tenure length
-- Expectation: No Results
SELECT 
    employee_id,
    start_date,
    end_date,
    tenure_length
FROM silver.hr_tenure
WHERE tenure_length < 0;

-- Check for reasonable tenure length (should not exceed 50 years)
-- Expectation: Review Results
SELECT 
    employee_id,
    start_date,
    end_date,
    tenure_length
FROM silver.hr_tenure
WHERE tenure_length > 50;

-- Check for referential integrity - employee_id
-- Expectation: No Results
SELECT 
    DISTINCT employee_id 
FROM silver.hr_tenure
WHERE employee_id NOT IN (
    SELECT employee_id 
    FROM silver.hr_demographics
);

-- ======================================
-- QUALITY CHECKS FOR silver.hr_training
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for required fields
SELECT 
    'training_id' AS column_name, COUNT(*) AS null_count 
FROM silver.hr_training 
WHERE training_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_training 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'training_title' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_training 
WHERE training_title IS NULL
UNION ALL
SELECT 
    'date' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_training 
WHERE date IS NULL
UNION ALL
SELECT 
    'training_hours' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_training 
WHERE training_hours IS NULL
UNION ALL
SELECT 
    'number_of_participants' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_training 
WHERE number_of_participants IS NULL
UNION ALL
SELECT 
    'total_training_hours' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_training 
WHERE total_training_hours IS NULL
UNION ALL
SELECT 
    'date_created' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_training 
WHERE date_created IS NULL
UNION ALL
SELECT 
    'date_updated' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_training 
WHERE date_updated IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    training_id, 
    COUNT(*) AS duplicate_count
FROM silver.hr_training
GROUP BY training_id
HAVING COUNT(*) > 1;

-- Check for negative or zero values
-- Expectation: No Results
SELECT 
    training_id,
    training_hours,
    number_of_participants,
    total_training_hours
FROM silver.hr_training
WHERE training_hours <= 0 OR number_of_participants <= 0 OR total_training_hours <= 0;

-- Check for reasonable training hours (should not exceed 200 hours per session)
-- Expectation: Review Results
SELECT 
    training_id,
    training_title,
    training_hours,
    number_of_participants
FROM silver.hr_training
WHERE training_hours > 200;

-- Check for total training hours calculation
-- Expectation: No Results
SELECT 
    training_id,
    training_hours,
    number_of_participants,
    total_training_hours,
    (training_hours * number_of_participants) AS calculated_total
FROM silver.hr_training
WHERE total_training_hours != (training_hours * number_of_participants);

-- Check for referential integrity - company_id
-- Expectation: No Results
SELECT 
    DISTINCT company_id 
FROM silver.hr_training
WHERE company_id NOT IN (
    SELECT company_id 
    FROM ref.company_main
);

-- ======================================
-- QUALITY CHECKS FOR silver.hr_safety_workdata
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for required fields
SELECT 
    'safety_workdata_id' AS column_name, COUNT(*) AS null_count 
FROM silver.hr_safety_workdata 
WHERE safety_workdata_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_safety_workdata 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'contractor' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_safety_workdata 
WHERE contractor IS NULL
UNION ALL
SELECT 
    'date' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_safety_workdata 
WHERE date IS NULL
UNION ALL
SELECT 
    'manpower' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_safety_workdata 
WHERE manpower IS NULL
UNION ALL
SELECT 
    'manhours' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_safety_workdata 
WHERE manhours IS NULL
UNION ALL
SELECT 
    'date_created' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_safety_workdata 
WHERE date_created IS NULL
UNION ALL
SELECT 
    'date_updated' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_safety_workdata 
WHERE date_updated IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    safety_workdata_id, 
    COUNT(*) AS duplicate_count
FROM silver.hr_safety_workdata
GROUP BY safety_workdata_id
HAVING COUNT(*) > 1;

-- Check for negative or zero values
-- Expectation: No Results
SELECT 
    safety_workdata_id,
    manpower,
    manhours
FROM silver.hr_safety_workdata
WHERE manpower <= 0 OR manhours <= 0;

-- Check for reasonable manpower values (should not exceed 10000)
-- Expectation: Review Results
SELECT 
    safety_workdata_id,
    company_id,
    manpower,
    manhours
FROM silver.hr_safety_workdata
WHERE manpower > 10000;

-- Check for reasonable manhours (should not exceed 100000 per record)
-- Expectation: Review Results
SELECT 
    safety_workdata_id,
    company_id,
    manpower,
    manhours
FROM silver.hr_safety_workdata
WHERE manhours > 100000;

-- Check for referential integrity - company_id
-- Expectation: No Results
SELECT 
    DISTINCT company_id 
FROM silver.hr_safety_workdata
WHERE company_id NOT IN (
    SELECT company_id 
    FROM ref.company_main
);

-- ======================================
-- QUALITY CHECKS FOR silver.hr_occupational_safety_health
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for required fields
SELECT 
    'osh_id' AS column_name, COUNT(*) AS null_count 
FROM silver.hr_occupational_safety_health 
WHERE osh_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_occupational_safety_health 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'workforce_type' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_occupational_safety_health 
WHERE workforce_type IS NULL
UNION ALL
SELECT 
    'lost_time' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_occupational_safety_health 
WHERE lost_time IS NULL
UNION ALL
SELECT 
    'date' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_occupational_safety_health 
WHERE date IS NULL
UNION ALL
SELECT 
    'incident_type' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_occupational_safety_health 
WHERE incident_type IS NULL
UNION ALL
SELECT 
    'incident_title' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_occupational_safety_health 
WHERE incident_title IS NULL
UNION ALL
SELECT 
    'incident_count' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_occupational_safety_health 
WHERE incident_count IS NULL
UNION ALL
SELECT 
    'date_created' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_occupational_safety_health 
WHERE date_created IS NULL
UNION ALL
SELECT 
    'date_updated' AS column_name, COUNT(*) AS null_count  
FROM silver.hr_occupational_safety_health 
WHERE date_updated IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    osh_id, 
    COUNT(*) AS duplicate_count
FROM silver.hr_occupational_safety_health
GROUP BY osh_id
HAVING COUNT(*) > 1;

-- Check for negative incident counts
-- Expectation: No Results
SELECT 
    osh_id,
    incident_count
FROM silver.hr_occupational_safety_health
WHERE incident_count < 0;

-- Check for referential integrity - company_id
-- Expectation: No Results
SELECT 
    DISTINCT company_id 
FROM silver.hr_occupational_safety_health
WHERE company_id NOT IN (
    SELECT company_id 
    FROM ref.company_main
);

-- Data standardization checks
SELECT DISTINCT 
    workforce_type 
FROM silver.hr_occupational_safety_health
ORDER BY workforce_type;

SELECT DISTINCT 
    incident_type 
FROM silver.hr_occupational_safety_health
ORDER BY incident_type;

-- ======================================
-- DATA ANALYSIS AND REPORTING QUERIES
-- ======================================

-- Employee demographics summary
SELECT 
    gender,
    COUNT(*) AS employee_count,
    AVG(EXTRACT(YEAR FROM AGE(birthdate))) AS avg_age,
    MIN(EXTRACT(YEAR FROM AGE(birthdate))) AS min_age,
    MAX(EXTRACT(YEAR FROM AGE(birthdate))) AS max_age
FROM silver.hr_demographics
WHERE birthdate IS NOT NULL
GROUP BY gender
ORDER BY gender;

-- Company-wise employee distribution
SELECT 
    hd.company_id,
    cm.company_name,
    COUNT(*) AS employee_count,
    COUNT(DISTINCT hd.position_id) AS unique_positions,
    COUNT(DISTINCT hd.employment_status) AS employment_statuses
FROM silver.hr_demographics hd
LEFT JOIN ref.company_main cm ON hd.company_id = cm.company_id
GROUP BY hd.company_id, cm.company_name
ORDER BY employee_count DESC;

-- Training summary by company
SELECT 
    ht.company_id,
    cm.company_name,
    COUNT(*) AS total_training_sessions,
    SUM(ht.number_of_participants) AS total_participants,
    SUM(ht.total_training_hours) AS total_training_hours,
    AVG(ht.training_hours) AS avg_hours_per_session,
    AVG(ht.number_of_participants) AS avg_participants_per_session
FROM silver.hr_training ht
LEFT JOIN ref.company_main cm ON ht.company_id = cm.company_id
GROUP BY ht.company_id, cm.company_name
ORDER BY total_training_hours DESC;

-- Safety workdata summary
SELECT 
    hsw.company_id,
    cm.company_name,
    COUNT(*) AS total_records,
    SUM(hsw.manpower) AS total_manpower,
    SUM(hsw.manhours) AS total_manhours,
    AVG(hsw.manpower) AS avg_manpower,
    AVG(hsw.manhours) AS avg_manhours
FROM silver.hr_safety_workdata hsw
LEFT JOIN ref.company_main cm ON hsw.company_id = cm.company_id
GROUP BY hsw.company_id, cm.company_name
ORDER BY total_manhours DESC;

-- Occupational safety incidents summary
SELECT 
    hosh.company_id,
    cm.company_name,
    hosh.incident_type,
    COUNT(*) AS incident_records,
    SUM(hosh.incident_count) AS total_incidents,
    COUNT(CASE WHEN hosh.lost_time = TRUE THEN 1 END) AS lost_time_incidents,
    COUNT(CASE WHEN hosh.lost_time = FALSE THEN 1 END) AS non_lost_time_incidents
FROM silver.hr_occupational_safety_health hosh
LEFT JOIN ref.company_main cm ON hosh.company_id = cm.company_id
GROUP BY hosh.company_id, cm.company_name, hosh.incident_type
ORDER BY total_incidents DESC;

-- Parental leave analysis
SELECT 
    hpl.type_of_leave,
    COUNT(*) AS leave_records,
    SUM(hpl.days) AS total_days,
    AVG(hpl.days) AS avg_days,
    MIN(hpl.days) AS min_days,
    MAX(hpl.days) AS max_days
FROM silver.hr_parental_leave hpl
GROUP BY hpl.type_of_leave
ORDER BY total_days DESC;

-- Tenure analysis
SELECT 
    AVG(tenure_length) AS avg_tenure_years,
    MIN(tenure_length) AS min_tenure_years,
    MAX(tenure_length) AS max_tenure_years,
    COUNT(*) AS total_tenure_records
FROM silver.hr_tenure;

-- Check for missing relationships
-- Employees without parental leave records
SELECT 
    COUNT(DISTINCT hd.employee_id) AS employees_without_parental_leave
FROM silver.hr_demographics hd
LEFT JOIN silver.hr_parental_leave hpl ON hd.employee_id = hpl.employee_id
WHERE hpl.employee_id IS NULL;

-- Employees without tenure records
SELECT 
    COUNT(DISTINCT hd.employee_id) AS employees_without_tenure
FROM silver.hr_demographics hd
LEFT JOIN silver.hr_tenure ht ON hd.employee_id = ht.employee_id
WHERE ht.employee_id IS NULL;
