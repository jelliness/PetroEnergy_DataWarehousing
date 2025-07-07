-- ======================================
-- QUALITY CHECKS FOR silver.csr_programs
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'program_id' AS column_name, COUNT(*) AS null_count 
FROM silver.csr_programs 
WHERE program_id IS NULL
UNION ALL
SELECT 
    'program_name' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_programs 
WHERE program_name IS NULL
UNION ALL
SELECT 
    'date_created' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_programs 
WHERE date_created IS NULL
UNION ALL
SELECT 
    'date_updated' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_programs 
WHERE date_updated IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    program_id, 
    COUNT(*) AS duplicate_count
FROM silver.csr_programs
GROUP BY program_id
HAVING COUNT(*) > 1;

-- Check for business logic duplicates (same program name)
-- Expectation: Review Results
SELECT 
    program_name,
    COUNT(*) AS duplicate_count
FROM silver.csr_programs
GROUP BY program_name
HAVING COUNT(*) > 1;

-- Check for unwanted whitespaces in string columns
-- Expectation: No Results
SELECT 
    'program_id' AS column_name, 
    program_id AS value
FROM silver.csr_programs
WHERE program_id != TRIM(program_id)
UNION ALL
SELECT 
    'program_name' AS column_name, 
    program_name AS value
FROM silver.csr_programs
WHERE program_name != TRIM(program_name);

-- Check for empty or very short program names
-- Expectation: Review Results
SELECT 
    program_id,
    program_name,
    LENGTH(program_name) AS name_length
FROM silver.csr_programs
WHERE LENGTH(TRIM(program_name)) < 3;

-- Check timestamp consistency
-- Expectation: No Results
SELECT 
    program_id,
    date_created,
    date_updated
FROM silver.csr_programs
WHERE date_created > date_updated;

-- ======================================
-- QUALITY CHECKS FOR silver.csr_projects
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'project_id' AS column_name, COUNT(*) AS null_count 
FROM silver.csr_projects 
WHERE project_id IS NULL
UNION ALL
SELECT 
    'program_id' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_projects 
WHERE program_id IS NULL
UNION ALL
SELECT 
    'project_name' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_projects 
WHERE project_name IS NULL
UNION ALL
SELECT 
    'project_metrics' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_projects 
WHERE project_metrics IS NULL
UNION ALL
SELECT 
    'date_created' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_projects 
WHERE date_created IS NULL
UNION ALL
SELECT 
    'date_updated' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_projects 
WHERE date_updated IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    project_id, 
    COUNT(*) AS duplicate_count
FROM silver.csr_projects
GROUP BY project_id
HAVING COUNT(*) > 1;

-- Check for business logic duplicates (same project name within same program)
-- Expectation: Review Results
SELECT 
    program_id,
    project_name,
    COUNT(*) AS duplicate_count
FROM silver.csr_projects
GROUP BY program_id, project_name
HAVING COUNT(*) > 1;

-- Check for unwanted whitespaces in string columns
-- Expectation: No Results
SELECT 
    'project_id' AS column_name, 
    project_id AS value
FROM silver.csr_projects
WHERE project_id != TRIM(project_id)
UNION ALL
SELECT 
    'program_id' AS column_name, 
    program_id AS value
FROM silver.csr_projects
WHERE program_id != TRIM(program_id)
UNION ALL
SELECT 
    'project_name' AS column_name, 
    project_name AS value
FROM silver.csr_projects
WHERE project_name != TRIM(project_name)
UNION ALL
SELECT 
    'project_metrics' AS column_name, 
    project_metrics AS value
FROM silver.csr_projects
WHERE project_metrics != TRIM(project_metrics);

-- Check for referential integrity
-- Make sure all program_ids exist in parent table
-- Expectation: No Results
SELECT 
    DISTINCT program_id 
FROM silver.csr_projects
WHERE program_id NOT IN (
    SELECT program_id 
    FROM silver.csr_programs
);

-- Check for empty or very short project names
-- Expectation: Review Results
SELECT 
    project_id,
    project_name,
    LENGTH(project_name) AS name_length
FROM silver.csr_projects
WHERE LENGTH(TRIM(project_name)) < 5;

-- Check timestamp consistency
-- Expectation: No Results
SELECT 
    project_id,
    date_created,
    date_updated
FROM silver.csr_projects
WHERE date_created > date_updated;

-- Data standardization check for project_metrics
-- This will show all unique values for review
SELECT DISTINCT 
    project_metrics 
FROM silver.csr_projects
ORDER BY project_metrics;

-- ======================================
-- QUALITY CHECKS FOR silver.csr_activity
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for required fields
SELECT 
    'csr_id' AS column_name, COUNT(*) AS null_count 
FROM silver.csr_activity 
WHERE csr_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_activity 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'project_id' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_activity 
WHERE project_id IS NULL
UNION ALL
SELECT 
    'project_year' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_activity 
WHERE project_year IS NULL
UNION ALL
SELECT 
    'csr_report' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_activity 
WHERE csr_report IS NULL
UNION ALL
SELECT 
    'project_expenses' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_activity 
WHERE project_expenses IS NULL
UNION ALL
SELECT 
    'date_created' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_activity 
WHERE date_created IS NULL
UNION ALL
SELECT 
    'date_updated' AS column_name, COUNT(*) AS null_count  
FROM silver.csr_activity 
WHERE date_updated IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    csr_id, 
    COUNT(*) AS duplicate_count
FROM silver.csr_activity
GROUP BY csr_id
HAVING COUNT(*) > 1;

-- Check for business logic duplicates (same company, project, year)
-- Expectation: Review Results
SELECT 
    company_id,
    project_id,
    project_year,
    COUNT(*) AS duplicate_count
FROM silver.csr_activity
GROUP BY company_id, project_id, project_year
HAVING COUNT(*) > 1;

-- Check for unwanted whitespaces in string columns
-- Expectation: No Results
SELECT 
    'csr_id' AS column_name, 
    csr_id AS value
FROM silver.csr_activity
WHERE csr_id != TRIM(csr_id)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.csr_activity
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'project_id' AS column_name, 
    project_id AS value
FROM silver.csr_activity
WHERE project_id != TRIM(project_id)
UNION ALL
SELECT 
    'project_remarks' AS column_name, 
    project_remarks AS value
FROM silver.csr_activity
WHERE project_remarks IS NOT NULL AND project_remarks != TRIM(project_remarks);

-- Check for negative values in numeric columns
-- Expectation: No Results (depending on business rules)
SELECT 
    csr_id,
    csr_report,
    'csr_report' AS column_name
FROM silver.csr_activity
WHERE csr_report < 0
UNION ALL
SELECT 
    csr_id,
    project_expenses,
    'project_expenses' AS column_name
FROM silver.csr_activity
WHERE project_expenses < 0;

-- Check for reasonable year values
-- Expectation: No Results for unreasonable years
SELECT 
    csr_id,
    project_year
FROM silver.csr_activity
WHERE project_year < 2000 OR project_year > EXTRACT(YEAR FROM CURRENT_DATE) + 1;

-- Check for referential integrity - company_id
-- Expectation: No Results
SELECT 
    DISTINCT company_id 
FROM silver.csr_activity
WHERE company_id NOT IN (
    SELECT company_id 
    FROM ref.company_main
);

-- Check for referential integrity - project_id
-- Expectation: No Results
SELECT 
    DISTINCT project_id 
FROM silver.csr_activity
WHERE project_id NOT IN (
    SELECT project_id 
    FROM silver.csr_projects
);

-- Check timestamp consistency
-- Expectation: No Results
SELECT 
    csr_id,
    date_created,
    date_updated
FROM silver.csr_activity
WHERE date_created > date_updated;

-- Check for zero values (might indicate missing data)
-- Expectation: Review Results
SELECT 
    'zero_csr_report' AS check_type,
    COUNT(*) AS count
FROM silver.csr_activity
WHERE csr_report = 0
UNION ALL
SELECT 
    'zero_project_expenses' AS check_type,
    COUNT(*) AS count
FROM silver.csr_activity
WHERE project_expenses = 0;

-- ======================================
-- DATA ANALYSIS AND REPORTING QUERIES
-- ======================================

-- Summary statistics for CSR activities
SELECT 
    'csr_report' AS metric,
    MIN(csr_report) AS min_value,
    MAX(csr_report) AS max_value,
    AVG(csr_report) AS avg_value,
    STDDEV(csr_report) AS stddev_value,
    COUNT(*) AS total_records
FROM silver.csr_activity
WHERE csr_report IS NOT NULL
UNION ALL
SELECT 
    'project_expenses' AS metric,
    MIN(project_expenses) AS min_value,
    MAX(project_expenses) AS max_value,
    AVG(project_expenses) AS avg_value,
    STDDEV(project_expenses) AS stddev_value,
    COUNT(*) AS total_records
FROM silver.csr_activity
WHERE project_expenses IS NOT NULL;

-- Year-wise CSR activity distribution
SELECT 
    project_year,
    COUNT(*) AS activity_count,
    COUNT(DISTINCT company_id) AS unique_companies,
    COUNT(DISTINCT project_id) AS unique_projects,
    SUM(csr_report) AS total_csr_report,
    SUM(project_expenses) AS total_expenses,
    AVG(csr_report) AS avg_csr_report,
    AVG(project_expenses) AS avg_expenses
FROM silver.csr_activity
WHERE project_year IS NOT NULL
GROUP BY project_year
ORDER BY project_year;

-- Company-wise CSR activity summary
SELECT 
    ca.company_id,
    cm.company_name,
    COUNT(*) AS total_activities,
    COUNT(DISTINCT ca.project_id) AS unique_projects,
    COUNT(DISTINCT ca.project_year) AS years_active,
    SUM(ca.csr_report) AS total_csr_report,
    SUM(ca.project_expenses) AS total_expenses,
    AVG(ca.csr_report) AS avg_csr_report,
    AVG(ca.project_expenses) AS avg_expenses
FROM silver.csr_activity ca
LEFT JOIN ref.company_main cm ON ca.company_id = cm.company_id
GROUP BY ca.company_id, cm.company_name
ORDER BY total_expenses DESC;

-- Program-wise analysis
SELECT 
    cp.program_id,
    cp.program_name,
    COUNT(DISTINCT cpj.project_id) AS total_projects,
    COUNT(DISTINCT ca.company_id) AS companies_involved,
    COUNT(ca.csr_id) AS total_activities,
    SUM(ca.csr_report) AS total_csr_report,
    SUM(ca.project_expenses) AS total_expenses
FROM silver.csr_programs cp
LEFT JOIN silver.csr_projects cpj ON cp.program_id = cpj.program_id
LEFT JOIN silver.csr_activity ca ON cpj.project_id = ca.project_id
GROUP BY cp.program_id, cp.program_name
ORDER BY total_expenses DESC;

-- Check for orphaned records
-- Programs with no projects
SELECT 
    cp.program_id,
    cp.program_name
FROM silver.csr_programs cp
LEFT JOIN silver.csr_projects cpj ON cp.program_id = cpj.program_id
WHERE cpj.program_id IS NULL;

-- Projects with no activities
SELECT 
    cpj.project_id,
    cpj.project_name,
    cpj.program_id
FROM silver.csr_projects cpj
LEFT JOIN silver.csr_activity ca ON cpj.project_id = ca.project_id
WHERE ca.project_id IS NULL;

-- Check for unusual expense-to-report ratios
-- This might indicate data entry errors
SELECT 
    csr_id,
    company_id,
    project_id,
    project_year,
    csr_report,
    project_expenses,
    CASE 
        WHEN csr_report > 0 THEN project_expenses / csr_report 
        ELSE NULL 
    END AS expense_to_report_ratio
FROM silver.csr_activity
WHERE csr_report > 0
    AND project_expenses > 0
    AND (
        (project_expenses / csr_report) > 10000  -- Very high expense ratio
        OR (project_expenses / csr_report) < 0.01  -- Very low expense ratio
    )
ORDER BY expense_to_report_ratio DESC;

-- Data completeness check - missing project remarks
SELECT 
    COUNT(*) AS total_activities,
    COUNT(project_remarks) AS activities_with_remarks,
    COUNT(*) - COUNT(project_remarks) AS activities_without_remarks,
    ROUND(
        (COUNT(project_remarks)::DECIMAL / COUNT(*)) * 100, 2
    ) AS remarks_completion_percentage
FROM silver.csr_activity;

-- Check for data consistency across related tables
-- Verify that program_id length matches expected format
SELECT 
    'csr_programs' AS table_name,
    MIN(LENGTH(program_id)) AS min_id_length,
    MAX(LENGTH(program_id)) AS max_id_length,
    COUNT(DISTINCT LENGTH(program_id)) AS unique_lengths
FROM silver.csr_programs
UNION ALL
SELECT 
    'csr_projects' AS table_name,
    MIN(LENGTH(project_id)) AS min_id_length,
    MAX(LENGTH(project_id)) AS max_id_length,
    COUNT(DISTINCT LENGTH(project_id)) AS unique_lengths
FROM silver.csr_projects
UNION ALL
SELECT 
    'csr_activity' AS table_name,
    MIN(LENGTH(csr_id)) AS min_id_length,
    MAX(LENGTH(csr_id)) AS max_id_length,
    COUNT(DISTINCT LENGTH(csr_id)) AS unique_lengths
FROM silver.csr_activity;
