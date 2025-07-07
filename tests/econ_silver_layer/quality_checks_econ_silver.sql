-- ======================================
-- QUALITY CHECKS FOR silver.econ_value
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results (except for some nullable columns)
SELECT 
    'year' AS column_name, COUNT(*) AS null_count 
FROM silver.econ_value 
WHERE year IS NULL
UNION ALL
SELECT 
    'electricity_sales' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_value 
WHERE electricity_sales IS NULL
UNION ALL
SELECT 
    'oil_revenues' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_value 
WHERE oil_revenues IS NULL
UNION ALL
SELECT 
    'other_revenues' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_value 
WHERE other_revenues IS NULL
UNION ALL
SELECT 
    'interest_income' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_value 
WHERE interest_income IS NULL
UNION ALL
SELECT 
    'share_in_net_income_of_associate' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_value 
WHERE share_in_net_income_of_associate IS NULL
UNION ALL
SELECT 
    'miscellaneous_income' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_value 
WHERE miscellaneous_income IS NULL
UNION ALL
SELECT 
    'total_revenue' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_value 
WHERE total_revenue IS NULL
UNION ALL
SELECT 
    'created_at' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_value 
WHERE created_at IS NULL
UNION ALL
SELECT 
    'updated_at' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_value 
WHERE updated_at IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    year, 
    COUNT(*) AS duplicate_count
FROM silver.econ_value
GROUP BY year
HAVING COUNT(*) > 1;

-- Check for negative values in revenue columns
-- Expectation: Review Results (some negative values might be acceptable)
SELECT 
    'electricity_sales' AS column_name,
    year,
    electricity_sales AS value
FROM silver.econ_value
WHERE electricity_sales < 0
UNION ALL
SELECT 
    'oil_revenues' AS column_name,
    year,
    oil_revenues AS value
FROM silver.econ_value
WHERE oil_revenues < 0
UNION ALL
SELECT 
    'other_revenues' AS column_name,
    year,
    other_revenues AS value
FROM silver.econ_value
WHERE other_revenues < 0
UNION ALL
SELECT 
    'interest_income' AS column_name,
    year,
    interest_income AS value
FROM silver.econ_value
WHERE interest_income < 0
UNION ALL
SELECT 
    'miscellaneous_income' AS column_name,
    year,
    miscellaneous_income AS value
FROM silver.econ_value
WHERE miscellaneous_income < 0;

-- Check for reasonable year values
-- Expectation: No Results for unreasonable years
SELECT 
    year,
    total_revenue
FROM silver.econ_value
WHERE year < 2000 OR year > EXTRACT(YEAR FROM CURRENT_DATE) + 1;

-- Check derived column calculation accuracy
-- Expectation: No Results (calculated field should match manual calculation)
SELECT 
    year,
    electricity_sales,
    oil_revenues,
    other_revenues,
    interest_income,
    share_in_net_income_of_associate,
    miscellaneous_income,
    total_revenue,
    (COALESCE(electricity_sales, 0) + COALESCE(oil_revenues, 0) + COALESCE(other_revenues, 0) + 
     COALESCE(interest_income, 0) + COALESCE(share_in_net_income_of_associate, 0) + 
     COALESCE(miscellaneous_income, 0)) AS calculated_total
FROM silver.econ_value
WHERE total_revenue != (COALESCE(electricity_sales, 0) + COALESCE(oil_revenues, 0) + COALESCE(other_revenues, 0) + 
                        COALESCE(interest_income, 0) + COALESCE(share_in_net_income_of_associate, 0) + 
                        COALESCE(miscellaneous_income, 0));

-- Check timestamp consistency
-- Expectation: No Results
SELECT 
    year,
    created_at,
    updated_at
FROM silver.econ_value
WHERE created_at > updated_at;

-- ======================================
-- QUALITY CHECKS FOR silver.econ_expenditures
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for required fields
SELECT 
    'year' AS column_name, COUNT(*) AS null_count 
FROM silver.econ_expenditures 
WHERE year IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_expenditures 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'type_id' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_expenditures 
WHERE type_id IS NULL
UNION ALL
SELECT 
    'government_payments' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_expenditures 
WHERE government_payments IS NULL
UNION ALL
SELECT 
    'supplier_spending_local' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_expenditures 
WHERE supplier_spending_local IS NULL
UNION ALL
SELECT 
    'supplier_spending_abroad' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_expenditures 
WHERE supplier_spending_abroad IS NULL
UNION ALL
SELECT 
    'employee_wages_benefits' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_expenditures 
WHERE employee_wages_benefits IS NULL
UNION ALL
SELECT 
    'community_investments' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_expenditures 
WHERE community_investments IS NULL
UNION ALL
SELECT 
    'depreciation' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_expenditures 
WHERE depreciation IS NULL
UNION ALL
SELECT 
    'depletion' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_expenditures 
WHERE depletion IS NULL
UNION ALL
SELECT 
    'others' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_expenditures 
WHERE others IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    year, 
    company_id,
    type_id,
    COUNT(*) AS duplicate_count
FROM silver.econ_expenditures
GROUP BY year, company_id, type_id
HAVING COUNT(*) > 1;

-- Check for negative values in expenditure columns
-- Expectation: Review Results (some negative values might be acceptable)
SELECT 
    'government_payments' AS column_name,
    year,
    company_id,
    government_payments AS value
FROM silver.econ_expenditures
WHERE government_payments < 0
UNION ALL
SELECT 
    'supplier_spending_local' AS column_name,
    year,
    company_id,
    supplier_spending_local AS value
FROM silver.econ_expenditures
WHERE supplier_spending_local < 0
UNION ALL
SELECT 
    'supplier_spending_abroad' AS column_name,
    year,
    company_id,
    supplier_spending_abroad AS value
FROM silver.econ_expenditures
WHERE supplier_spending_abroad < 0
UNION ALL
SELECT 
    'employee_wages_benefits' AS column_name,
    year,
    company_id,
    employee_wages_benefits AS value
FROM silver.econ_expenditures
WHERE employee_wages_benefits < 0
UNION ALL
SELECT 
    'community_investments' AS column_name,
    year,
    company_id,
    community_investments AS value
FROM silver.econ_expenditures
WHERE community_investments < 0
UNION ALL
SELECT 
    'depreciation' AS column_name,
    year,
    company_id,
    depreciation AS value
FROM silver.econ_expenditures
WHERE depreciation < 0
UNION ALL
SELECT 
    'depletion' AS column_name,
    year,
    company_id,
    depletion AS value
FROM silver.econ_expenditures
WHERE depletion < 0
UNION ALL
SELECT 
    'others' AS column_name,
    year,
    company_id,
    others AS value
FROM silver.econ_expenditures
WHERE others < 0;

-- Check for unwanted whitespaces in string columns
-- Expectation: No Results
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.econ_expenditures
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'type_id' AS column_name, 
    type_id AS value
FROM silver.econ_expenditures
WHERE type_id != TRIM(type_id);

-- Check for reasonable year values
-- Expectation: No Results for unreasonable years
SELECT 
    year,
    company_id,
    type_id,
    total_expenditures
FROM silver.econ_expenditures
WHERE year < 2000 OR year > EXTRACT(YEAR FROM CURRENT_DATE) + 1;

-- Check derived column calculations
-- Expectation: No Results (calculated fields should match manual calculations)
SELECT 
    year,
    company_id,
    type_id,
    supplier_spending_local,
    supplier_spending_abroad,
    total_supplier_spending,
    (COALESCE(supplier_spending_local, 0) + COALESCE(supplier_spending_abroad, 0)) AS calculated_supplier_total
FROM silver.econ_expenditures
WHERE total_supplier_spending != (COALESCE(supplier_spending_local, 0) + COALESCE(supplier_spending_abroad, 0));

-- Check referential integrity - company_id
-- Expectation: No Results
SELECT 
    DISTINCT company_id 
FROM silver.econ_expenditures
WHERE company_id NOT IN (
    SELECT company_id 
    FROM ref.company_main
);

-- Check referential integrity - type_id
-- Expectation: No Results
SELECT 
    DISTINCT type_id 
FROM silver.econ_expenditures
WHERE type_id NOT IN (
    SELECT type_id 
    FROM ref.expenditure_type
);

-- ======================================
-- QUALITY CHECKS FOR silver.econ_capital_provider_payment
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for required fields
SELECT 
    'year' AS column_name, COUNT(*) AS null_count 
FROM silver.econ_capital_provider_payment 
WHERE year IS NULL
UNION ALL
SELECT 
    'interest' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_capital_provider_payment 
WHERE interest IS NULL
UNION ALL
SELECT 
    'dividends_to_nci' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_capital_provider_payment 
WHERE dividends_to_nci IS NULL
UNION ALL
SELECT 
    'dividends_to_parent' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_capital_provider_payment 
WHERE dividends_to_parent IS NULL
UNION ALL
SELECT 
    'total_dividends_interest' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_capital_provider_payment 
WHERE total_dividends_interest IS NULL
UNION ALL
SELECT 
    'created_at' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_capital_provider_payment 
WHERE created_at IS NULL
UNION ALL
SELECT 
    'updated_at' AS column_name, COUNT(*) AS null_count  
FROM silver.econ_capital_provider_payment 
WHERE updated_at IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    year, 
    COUNT(*) AS duplicate_count
FROM silver.econ_capital_provider_payment
GROUP BY year
HAVING COUNT(*) > 1;

-- Check for negative values
-- Expectation: Review Results (some negative values might be acceptable)
SELECT 
    'interest' AS column_name,
    year,
    interest AS value
FROM silver.econ_capital_provider_payment
WHERE interest < 0
UNION ALL
SELECT 
    'dividends_to_nci' AS column_name,
    year,
    dividends_to_nci AS value
FROM silver.econ_capital_provider_payment
WHERE dividends_to_nci < 0
UNION ALL
SELECT 
    'dividends_to_parent' AS column_name,
    year,
    dividends_to_parent AS value
FROM silver.econ_capital_provider_payment
WHERE dividends_to_parent < 0;

-- Check derived column calculation
-- Expectation: No Results
SELECT 
    year,
    interest,
    dividends_to_nci,
    dividends_to_parent,
    total_dividends_interest,
    (COALESCE(interest, 0) + COALESCE(dividends_to_nci, 0) + COALESCE(dividends_to_parent, 0)) AS calculated_total
FROM silver.econ_capital_provider_payment
WHERE total_dividends_interest != (COALESCE(interest, 0) + COALESCE(dividends_to_nci, 0) + COALESCE(dividends_to_parent, 0));

-- Check for reasonable year values
-- Expectation: No Results for unreasonable years
SELECT 
    year,
    total_dividends_interest
FROM silver.econ_capital_provider_payment
WHERE year < 2000 OR year > EXTRACT(YEAR FROM CURRENT_DATE) + 1;

-- ======================================
-- DATA ANALYSIS AND REPORTING QUERIES
-- ======================================

-- Summary statistics for economic value
SELECT 
    'electricity_sales' AS metric,
    MIN(electricity_sales) AS min_value,
    MAX(electricity_sales) AS max_value,
    AVG(electricity_sales) AS avg_value,
    STDDEV(electricity_sales) AS stddev_value,
    COUNT(*) AS total_records
FROM silver.econ_value
WHERE electricity_sales IS NOT NULL
UNION ALL
SELECT 
    'oil_revenues' AS metric,
    MIN(oil_revenues) AS min_value,
    MAX(oil_revenues) AS max_value,
    AVG(oil_revenues) AS avg_value,
    STDDEV(oil_revenues) AS stddev_value,
    COUNT(*) AS total_records
FROM silver.econ_value
WHERE oil_revenues IS NOT NULL
UNION ALL
SELECT 
    'total_revenue' AS metric,
    MIN(total_revenue) AS min_value,
    MAX(total_revenue) AS max_value,
    AVG(total_revenue) AS avg_value,
    STDDEV(total_revenue) AS stddev_value,
    COUNT(*) AS total_records
FROM silver.econ_value
WHERE total_revenue IS NOT NULL;

-- Year-wise economic value analysis
SELECT 
    year,
    electricity_sales,
    oil_revenues,
    other_revenues,
    interest_income,
    share_in_net_income_of_associate,
    miscellaneous_income,
    total_revenue
FROM silver.econ_value
ORDER BY year;

-- Company-wise expenditure analysis
SELECT 
    company_id,
    cm.company_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT year) AS years_covered,
    COUNT(DISTINCT type_id) AS expenditure_types,
    SUM(total_expenditures) AS total_expenditures,
    AVG(total_expenditures) AS avg_expenditures,
    SUM(employee_wages_benefits) AS total_wages_benefits,
    SUM(community_investments) AS total_community_investments
FROM silver.econ_expenditures ee
LEFT JOIN ref.company_main cm ON ee.company_id = cm.company_id
GROUP BY company_id, cm.company_name
ORDER BY total_expenditures DESC;

-- Expenditure type analysis
SELECT 
    type_id,
    et.type_description,
    COUNT(*) AS total_records,
    COUNT(DISTINCT company_id) AS companies_involved,
    COUNT(DISTINCT year) AS years_covered,
    SUM(total_expenditures) AS total_expenditures,
    AVG(total_expenditures) AS avg_expenditures
FROM silver.econ_expenditures ee
LEFT JOIN ref.expenditure_type et ON ee.type_id = et.type_id
GROUP BY type_id, et.type_description
ORDER BY total_expenditures DESC;

-- Year-wise capital provider payment analysis
SELECT 
    year,
    interest,
    dividends_to_nci,
    dividends_to_parent,
    total_dividends_interest
FROM silver.econ_capital_provider_payment
ORDER BY year;

-- Check for zero values (might indicate missing data)
SELECT 
    'zero_total_revenue' AS check_type,
    COUNT(*) AS count
FROM silver.econ_value
WHERE total_revenue = 0
UNION ALL
SELECT 
    'zero_total_expenditures' AS check_type,
    COUNT(*) AS count
FROM silver.econ_expenditures
WHERE total_expenditures = 0
UNION ALL
SELECT 
    'zero_total_dividends_interest' AS check_type,
    COUNT(*) AS count
FROM silver.econ_capital_provider_payment
WHERE total_dividends_interest = 0;

-- Check for missing years (data gaps)
WITH year_range AS (
    SELECT generate_series(
        (SELECT MIN(year) FROM silver.econ_value),
        (SELECT MAX(year) FROM silver.econ_value)
    ) AS year
)
SELECT 
    yr.year AS missing_year
FROM year_range yr
LEFT JOIN silver.econ_value ev ON yr.year = ev.year
WHERE ev.year IS NULL;

-- Data completeness check
SELECT 
    'econ_value' AS table_name,
    COUNT(*) AS total_records,
    MIN(year) AS earliest_year,
    MAX(year) AS latest_year,
    MAX(year) - MIN(year) + 1 AS expected_years,
    COUNT(*) AS actual_years,
    (MAX(year) - MIN(year) + 1) - COUNT(*) AS missing_years
FROM silver.econ_value
UNION ALL
SELECT 
    'econ_expenditures' AS table_name,
    COUNT(*) AS total_records,
    MIN(year) AS earliest_year,
    MAX(year) AS latest_year,
    NULL AS expected_years,
    COUNT(DISTINCT year) AS actual_years,
    NULL AS missing_years
FROM silver.econ_expenditures
UNION ALL
SELECT 
    'econ_capital_provider_payment' AS table_name,
    COUNT(*) AS total_records,
    MIN(year) AS earliest_year,
    MAX(year) AS latest_year,
    MAX(year) - MIN(year) + 1 AS expected_years,
    COUNT(*) AS actual_years,
    (MAX(year) - MIN(year) + 1) - COUNT(*) AS missing_years
FROM silver.econ_capital_provider_payment;
