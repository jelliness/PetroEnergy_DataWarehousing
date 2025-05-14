-- ======================================
-- QUALITY CHECKS FOR envi_company_info
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_company_info 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'company_name' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_company_info 
WHERE company_name IS NULL
UNION ALL
SELECT 
    'resources' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_company_info 
WHERE resources IS NULL
UNION ALL
SELECT 
    'site_name' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_company_info 
WHERE site_name IS NULL
UNION ALL
SELECT 
    'site_address' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_company_info 
WHERE site_address IS NULL
UNION ALL
SELECT 
    'city_town' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_company_info 
WHERE city_town IS NULL
UNION ALL
SELECT 
    'province' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_company_info 
WHERE province IS NULL
UNION ALL
SELECT 
    'zip' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_company_info 
WHERE zip IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    company_id, 
    COUNT(*) AS duplicate_count
FROM silver.envi_company_info
GROUP BY company_id
HAVING COUNT(*) > 1;

SELECT 
    company_name,resources,site_name,site_address,city_town,province,zip,
    COUNT(*) AS duplicate_count
FROM silver.envi_company_info
GROUP BY 
    company_name,resources,site_name,site_address,city_town,province,zip
HAVING COUNT(*) > 1; 
 

-- Check for unwanted whitespaces in all columns
-- Expectation: No Results
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.envi_company_info
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'company_name' AS column_name, 
    company_name AS value
FROM silver.envi_company_info
WHERE company_name != TRIM(company_name)
UNION ALL
SELECT 
    'resources' AS column_name, 
    resources AS value
FROM silver.envi_company_info
WHERE resources != TRIM(resources)
UNION ALL
SELECT 
    'site_name' AS column_name, 
    site_name AS value
FROM silver.envi_company_info
WHERE site_name != TRIM(site_name)
UNION ALL
SELECT 
    'site_address' AS column_name, 
    site_address AS value
FROM silver.envi_company_info
WHERE site_address != TRIM(site_address)
UNION ALL
SELECT 
    'city_town' AS column_name, 
    city_town AS value
FROM silver.envi_company_info
WHERE city_town != TRIM(city_town)
UNION ALL
SELECT 
    'province' AS column_name, 
    province AS value
FROM silver.envi_company_info
WHERE province != TRIM(province)
UNION ALL
SELECT 
    'zip' AS column_name, 
    zip AS value
FROM silver.envi_company_info
WHERE zip != TRIM(zip);

-- ======================================
-- QUALITY CHECKS FOR envi_company_property
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'cp_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_company_property 
WHERE cp_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_company_property 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'cp_name' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_company_property 
WHERE cp_name IS NULL
UNION ALL
SELECT 
    'cp_type' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_company_property 
WHERE cp_type IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    cp_id, 
    COUNT(*) AS duplicate_count
FROM silver.envi_company_property
GROUP BY cp_id
HAVING COUNT(*) > 1;

SELECT 
    company_id, 
    cp_name, 
    cp_type,
    COUNT(*) AS duplicate_count
FROM silver.envi_company_property
GROUP BY 
    company_id, 
    cp_name, 
    cp_type
HAVING COUNT(*) > 1; 

-- Check for unwanted whitespaces in all columns
-- Expectation: No Results
SELECT 
    'cp_id' AS column_name, 
    cp_id AS value
FROM silver.envi_company_property
WHERE cp_id != TRIM(cp_id)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.envi_company_property
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'cp_name' AS column_name, 
    cp_name AS value
FROM silver.envi_company_property
WHERE cp_name != TRIM(cp_name)
UNION ALL
SELECT 
    'cp_type' AS column_name, 
    cp_type AS value
FROM silver.envi_company_property
WHERE cp_type != TRIM(cp_type);

-- Data standardization and consistency
-- This will show all unique values for categorical fields
SELECT DISTINCT 
    cp_type 
FROM silver.envi_company_property;

-- Check for referential integrity
-- Make sure all company_ids exist in parent table
-- Expectation: No Results
SELECT 
    company_id 
FROM silver.envi_company_property
WHERE company_id NOT IN (SELECT company_id FROM silver.envi_company_info);

-- ======================================
-- QUALITY CHECKS FOR envi_natural_sources
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'ns_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_natural_sources 
WHERE ns_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_natural_sources 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'ns_name' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_natural_sources 
WHERE ns_name IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    ns_id, 
    COUNT(*) AS duplicate_count
FROM silver.envi_natural_sources
GROUP BY ns_id
HAVING COUNT(*) > 1;

SELECT 
	company_id,ns_name,
    COUNT(*) AS duplicate_count
FROM silver.envi_natural_sources
GROUP BY 
	company_id,ns_name
HAVING COUNT(*) > 1; 

-- Check for unwanted whitespaces in all columns
-- Expectation: No Results
SELECT 
    'ns_id' AS column_name, 
    ns_id AS value
FROM silver.envi_natural_sources
WHERE ns_id != TRIM(ns_id)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.envi_natural_sources
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'ns_name' AS column_name, 
    ns_name AS value
FROM silver.envi_natural_sources
WHERE ns_name != TRIM(ns_name);

-- Data standardization and consistency
-- This will show all unique values for names
SELECT DISTINCT 
    ns_name 
FROM silver.envi_natural_sources;

-- Check for referential integrity
-- Make sure all company_ids exist in parent table
-- Expectation: No Results
SELECT 
    company_id 
FROM silver.envi_natural_sources
WHERE company_id NOT IN (SELECT company_id FROM silver.envi_company_info);

-- ======================================
-- QUALITY CHECKS FOR envi_water_withdrawal
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'ww_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_water_withdrawal 
WHERE ww_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_withdrawal 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'year' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_withdrawal 
WHERE year IS NULL
UNION ALL
SELECT 
    'month' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_withdrawal 
WHERE month IS NULL
UNION ALL
SELECT 
    'ns_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_withdrawal 
WHERE ns_id IS NULL
UNION ALL
SELECT 
    'volume' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_withdrawal 
WHERE volume IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_withdrawal 
WHERE unit_of_measurement IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    ww_id, 
    COUNT(*) AS duplicate_count
FROM silver.envi_water_withdrawal
GROUP BY ww_id
HAVING COUNT(*) > 1;

SELECT 
    company_id,year,month,ns_id,volume,unit_of_measurement,
    COUNT(*) AS duplicate_count
FROM silver.envi_water_withdrawal
GROUP BY 
    company_id,year,month,ns_id,volume,unit_of_measurement
HAVING COUNT(*) > 1; 

-- Check for unwanted whitespaces in all columns
-- Expectation: No Results
SELECT 
    'ww_id' AS column_name, 
    ww_id AS value
FROM silver.envi_water_withdrawal
WHERE ww_id != TRIM(ww_id)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.envi_water_withdrawal
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'month' AS column_name, 
    month AS value
FROM silver.envi_water_withdrawal
WHERE month != TRIM(month)
UNION ALL
SELECT 
    'ns_id' AS column_name, 
    ns_id AS value
FROM silver.envi_water_withdrawal
WHERE ns_id != TRIM(ns_id)
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, 
    unit_of_measurement AS value
FROM silver.envi_water_withdrawal
WHERE unit_of_measurement != TRIM(unit_of_measurement);

-- Check for negative values in numeric columns
-- Expectation: No Results
SELECT 
    ww_id,
    volume
FROM silver.envi_water_withdrawal
WHERE volume < 0;

SELECT 
    ww_id,
    year
FROM silver.envi_water_withdrawal
WHERE year < 0;

-- Data standardization and consistency
-- This will show all unique values for key fields
SELECT DISTINCT 
    unit_of_measurement 
FROM silver.envi_water_withdrawal;

SELECT DISTINCT 
    month 
FROM silver.envi_water_withdrawal;

-- Check for referential integrity
-- Expectation: No Results
SELECT 
    company_id 
FROM silver.envi_water_withdrawal
WHERE company_id NOT IN (SELECT company_id FROM silver.envi_company_info);

SELECT 
    ns_id 
FROM silver.envi_water_withdrawal
WHERE ns_id NOT IN (SELECT ns_id FROM silver.envi_natural_sources);

-- ======================================
-- QUALITY CHECKS FOR envi_diesel_consumption
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'dc_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_diesel_consumption 
WHERE dc_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_diesel_consumption 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'cp_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_diesel_consumption 
WHERE cp_id IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_diesel_consumption 
WHERE unit_of_measurement IS NULL
UNION ALL
SELECT 
    'consumption' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_diesel_consumption 
WHERE consumption IS NULL
UNION ALL
SELECT 
    'date' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_diesel_consumption 
WHERE date IS NULL
UNION ALL
SELECT 
    'month' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_diesel_consumption 
WHERE month IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    dc_id, 
    COUNT(*) AS duplicate_count
FROM silver.envi_diesel_consumption
GROUP BY dc_id
HAVING COUNT(*) > 1;

SELECT 
    company_id,cp_id,unit_of_measurement,consumption,date,month,
    COUNT(*) AS duplicate_count
FROM silver.envi_diesel_consumption
GROUP BY 
    company_id,cp_id,unit_of_measurement,consumption,date,month
HAVING COUNT(*) > 1; 

-- Check for unwanted whitespaces in all columns
-- Expectation: No Results
SELECT 
    'dc_id' AS column_name, 
    dc_id AS value
FROM silver.envi_diesel_consumption
WHERE dc_id != TRIM(dc_id)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.envi_diesel_consumption
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'cp_id' AS column_name, 
    cp_id AS value
FROM silver.envi_diesel_consumption
WHERE cp_id != TRIM(cp_id)
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, 
    unit_of_measurement AS value
FROM silver.envi_diesel_consumption
WHERE unit_of_measurement != TRIM(unit_of_measurement)
UNION ALL
SELECT 
    'month' AS column_name, 
    month AS value
FROM silver.envi_diesel_consumption
WHERE month != TRIM(month);

-- Check for negative values in numeric columns
-- Expectation: No Results
SELECT 
    dc_id,
    consumption
FROM silver.envi_diesel_consumption
WHERE consumption < 0;

-- Data standardization and consistency
-- This will show all unique values for key fields
SELECT DISTINCT 
    unit_of_measurement 
FROM silver.envi_diesel_consumption;

SELECT DISTINCT 
    month 
FROM silver.envi_diesel_consumption;

-- Check for referential integrity
-- Expectation: No Results
SELECT 
    company_id 
FROM silver.envi_diesel_consumption
WHERE company_id NOT IN (SELECT company_id FROM silver.envi_company_info);

SELECT 
    cp_id 
FROM silver.envi_diesel_consumption
WHERE cp_id NOT IN (SELECT cp_id FROM silver.envi_company_property);

-- ======================================
-- QUALITY CHECKS FOR envi_electric_consumption
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'ec_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_electric_consumption 
WHERE ec_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_electric_consumption 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_electric_consumption 
WHERE unit_of_measurement IS NULL
UNION ALL
SELECT 
    'consumption' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_electric_consumption 
WHERE consumption IS NULL
UNION ALL
SELECT 
    'quarter' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_electric_consumption 
WHERE quarter IS NULL
UNION ALL
SELECT 
    'year' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_electric_consumption 
WHERE year IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    ec_id, 
    COUNT(*) AS duplicate_count
FROM silver.envi_electric_consumption
GROUP BY ec_id
HAVING COUNT(*) > 1;

SELECT 
    company_id, unit_of_measurement, consumption, quarter, year,
    COUNT(*) AS duplicate_count
FROM silver.envi_electric_consumption
GROUP BY 
    company_id, unit_of_measurement, consumption, quarter, year
HAVING COUNT(*) > 1; 

-- Check for unwanted whitespaces in all columns
-- Expectation: No Results
SELECT 
    'ec_id' AS column_name, 
    ec_id AS value
FROM silver.envi_electric_consumption
WHERE ec_id != TRIM(ec_id)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.envi_electric_consumption
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, 
    unit_of_measurement AS value
FROM silver.envi_electric_consumption
WHERE unit_of_measurement != TRIM(unit_of_measurement)
UNION ALL
SELECT 
    'quarter' AS column_name, 
    quarter AS value
FROM silver.envi_electric_consumption
WHERE quarter != TRIM(quarter);

-- Check for negative values in numeric columns
-- Expectation: No Results
SELECT 
    ec_id,
    consumption
FROM silver.envi_electric_consumption
WHERE consumption < 0;

SELECT 
    ec_id,
    year
FROM silver.envi_electric_consumption
WHERE year < 0;

-- Data standardization and consistency
-- This will show all unique values for key fields
SELECT DISTINCT 
    unit_of_measurement 
FROM silver.envi_electric_consumption;

SELECT DISTINCT 
    quarter 
FROM silver.envi_electric_consumption;

-- Check for referential integrity
-- Expectation: No Results
SELECT 
    company_id 
FROM silver.envi_electric_consumption
WHERE company_id NOT IN (SELECT company_id FROM silver.envi_company_info);

-- ======================================
-- QUALITY CHECKS FOR envi_power_generation
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'pg_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_power_generation 
WHERE pg_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_power_generation 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_power_generation 
WHERE unit_of_measurement IS NULL
UNION ALL
SELECT 
    'generation' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_power_generation 
WHERE generation IS NULL
UNION ALL
SELECT 
    'quarter' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_power_generation 
WHERE quarter IS NULL
UNION ALL
SELECT 
    'year' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_power_generation 
WHERE year IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    pg_id, 
    COUNT(*) AS duplicate_count
FROM silver.envi_power_generation
GROUP BY pg_id
HAVING COUNT(*) > 1;

SELECT 
    company_id, unit_of_measurement, generation, quarter, year,
    COUNT(*) AS duplicate_count
FROM silver.envi_power_generation
GROUP BY 
    company_id, unit_of_measurement, generation, quarter, year
HAVING COUNT(*) > 1; 

-- Check for unwanted whitespaces in all columns
-- Expectation: No Results
SELECT 
    'pg_id' AS column_name, 
    pg_id AS value
FROM silver.envi_power_generation
WHERE pg_id != TRIM(pg_id)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.envi_power_generation
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, 
    unit_of_measurement AS value
FROM silver.envi_power_generation
WHERE unit_of_measurement != TRIM(unit_of_measurement)
UNION ALL
SELECT 
    'quarter' AS column_name, 
    quarter AS value
FROM silver.envi_power_generation
WHERE quarter != TRIM(quarter);

-- Check for negative values in numeric columns
-- Expectation: No Results
SELECT 
    pg_id,
    generation
FROM silver.envi_power_generation
WHERE generation < 0;

SELECT 
    pg_id,
    year
FROM silver.envi_power_generation
WHERE year < 0;

-- Data standardization and consistency
-- This will show all unique values for key fields
SELECT DISTINCT 
    unit_of_measurement 
FROM silver.envi_power_generation;

SELECT DISTINCT 
    quarter 
FROM silver.envi_power_generation;

-- Check for referential integrity
-- Expectation: No Results
SELECT 
    company_id 
FROM silver.envi_power_generation
WHERE company_id NOT IN (SELECT company_id FROM silver.envi_company_info);

-- ======================================
-- QUALITY CHECKS FOR envi_non_hazard_waste
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'nhw_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_non_hazard_waste 
WHERE nhw_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_non_hazard_waste 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'waste_source' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_non_hazard_waste 
WHERE waste_source IS NULL
UNION ALL
SELECT 
    'metrics' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_non_hazard_waste 
WHERE metrics IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_non_hazard_waste 
WHERE unit_of_measurement IS NULL
UNION ALL
SELECT 
    'waste' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_non_hazard_waste 
WHERE waste IS NULL
UNION ALL
SELECT 
    'month' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_non_hazard_waste 
WHERE month IS NULL
UNION ALL
SELECT 
    'year' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_non_hazard_waste 
WHERE year IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    nhw_id, 
    COUNT(*) AS duplicate_count
FROM silver.envi_non_hazard_waste
GROUP BY nhw_id
HAVING COUNT(*) > 1;

SELECT 
    company_id, waste_source, metrics, unit_of_measurement, waste, month, year,
    COUNT(*) AS duplicate_count
FROM silver.envi_non_hazard_waste
GROUP BY 
    company_id, waste_source, metrics, unit_of_measurement, waste, month, year
HAVING COUNT(*) > 1; 

-- Check for unwanted whitespaces in all columns
-- Expectation: No Results
SELECT 
    'nhw_id' AS column_name, 
    nhw_id AS value
FROM silver.envi_non_hazard_waste
WHERE nhw_id != TRIM(nhw_id)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.envi_non_hazard_waste
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'waste_source' AS column_name, 
    waste_source AS value
FROM silver.envi_non_hazard_waste
WHERE waste_source != TRIM(waste_source)
UNION ALL
SELECT 
    'metrics' AS column_name, 
    metrics AS value
FROM silver.envi_non_hazard_waste
WHERE metrics != TRIM(metrics)
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, 
    unit_of_measurement AS value
FROM silver.envi_non_hazard_waste
WHERE unit_of_measurement != TRIM(unit_of_measurement)
UNION ALL
SELECT 
    'month' AS column_name, 
    month AS value
FROM silver.envi_non_hazard_waste
WHERE month != TRIM(month);

-- Check for negative values in numeric columns
-- Expectation: No Results
SELECT 
    nhw_id,
    waste
FROM silver.envi_non_hazard_waste
WHERE waste < 0;

SELECT 
    nhw_id,
    year
FROM silver.envi_non_hazard_waste
WHERE year < 0;

-- Data standardization and consistency
-- This will show all unique values for key categorical fields
SELECT DISTINCT 
    waste_source 
FROM silver.envi_non_hazard_waste;

SELECT DISTINCT 
    metrics 
FROM silver.envi_non_hazard_waste;

SELECT DISTINCT 
    unit_of_measurement 
FROM silver.envi_non_hazard_waste;

SELECT DISTINCT 
    month 
FROM silver.envi_non_hazard_waste;

-- Check for referential integrity
-- Expectation: No Results
SELECT 
    company_id 
FROM silver.envi_non_hazard_waste
WHERE company_id NOT IN (SELECT company_id FROM silver.envi_company_info);

-- ======================================
-- QUALITY CHECKS FOR envi_hazard_waste
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'hw_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_hazard_waste 
WHERE hw_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'metrics' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste 
WHERE metrics IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste 
WHERE unit_of_measurement IS NULL
UNION ALL
SELECT 
    'waste' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste 
WHERE waste IS NULL
UNION ALL
SELECT 
    'quarter' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste 
WHERE quarter IS NULL
UNION ALL
SELECT 
    'year' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste 
WHERE year IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    hw_id, 
    COUNT(*) AS duplicate_count
FROM silver.envi_hazard_waste
GROUP BY hw_id
HAVING COUNT(*) > 1;

SELECT 
    company_id, metrics, unit_of_measurement, waste, quarter, year,
    COUNT(*) AS duplicate_count
FROM silver.envi_hazard_waste
GROUP BY 
    company_id, metrics, unit_of_measurement, waste, quarter, year
HAVING COUNT(*) > 1; 

-- Check for unwanted whitespaces in all columns
-- Expectation: No Results
SELECT 
    'hw_id' AS column_name, 
    hw_id AS value
FROM silver.envi_hazard_waste
WHERE hw_id != TRIM(hw_id)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.envi_hazard_waste
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'metrics' AS column_name, 
    metrics AS value
FROM silver.envi_hazard_waste
WHERE metrics != TRIM(metrics)
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, 
    unit_of_measurement AS value
FROM silver.envi_hazard_waste
WHERE unit_of_measurement != TRIM(unit_of_measurement)
UNION ALL
SELECT 
    'quarter' AS column_name, 
    quarter AS value
FROM silver.envi_hazard_waste
WHERE quarter != TRIM(quarter);

-- Check for negative values in numeric columns
-- Expectation: No Results
SELECT 
    hw_id,
    waste
FROM silver.envi_hazard_waste
WHERE waste < 0;

SELECT 
    hw_id,
    year
FROM silver.envi_hazard_waste
WHERE year < 0;

-- Data standardization and consistency
-- This will show all unique values for key categorical fields
SELECT DISTINCT 
    metrics 
FROM silver.envi_hazard_waste;

SELECT DISTINCT 
    unit_of_measurement 
FROM silver.envi_hazard_waste;

SELECT DISTINCT 
    quarter 
FROM silver.envi_hazard_waste;

-- Check for referential integrity
-- Expectation: No Results
SELECT 
    company_id 
FROM silver.envi_hazard_waste
WHERE company_id NOT IN (SELECT company_id FROM silver.envi_company_info);

-- ======================================
-- QUALITY CHECKS FOR envi_activity
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'ea_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_activity 
WHERE ea_id IS NULL
UNION ALL
SELECT 
    'metrics' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_activity 
WHERE metrics IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_activity 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'envi_act_name' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_activity 
WHERE envi_act_name IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    ea_id, 
    COUNT(*) AS duplicate_count
FROM silver.envi_activity
GROUP BY ea_id
HAVING COUNT(*) > 1;

SELECT 
    metrics,company_id,envi_act_name,
    COUNT(*) AS duplicate_count
FROM silver.envi_activity
GROUP BY 
    metrics,company_id,envi_act_name
HAVING COUNT(*) > 1; 

-- Check for unwanted whitespaces in all columns
-- Expectation: No Results
SELECT 
    'ea_id' AS column_name, 
    ea_id AS value
FROM silver.envi_activity
WHERE ea_id != TRIM(ea_id)
UNION ALL
SELECT 
    'metrics' AS column_name, 
    metrics AS value
FROM silver.envi_activity
WHERE metrics != TRIM(metrics)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.envi_activity
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'envi_act_name' AS column_name, 
    envi_act_name AS value
FROM silver.envi_activity
WHERE envi_act_name != TRIM(envi_act_name);

-- Data standardization and consistency
-- This will show all unique values for key categorical fields
SELECT DISTINCT 
    metrics 
FROM silver.envi_activity;

-- Check for referential integrity
-- Expectation: No Results
SELECT 
    company_id 
FROM silver.envi_activity
WHERE company_id NOT IN (SELECT company_id FROM silver.envi_company_info);

-- ======================================
-- QUALITY CHECKS FOR envi_activity_output
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results
SELECT 
    'eao_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_activity_output 
WHERE eao_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_activity_output 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'ea_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_activity_output 
WHERE ea_id IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_activity_output 
WHERE unit_of_measurement IS NULL
UNION ALL
SELECT 
    'act_output' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_activity_output 
WHERE act_output IS NULL
UNION ALL
SELECT 
    'year' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_activity_output 
WHERE year IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    eao_id, 
    COUNT(*) AS duplicate_count
FROM silver.envi_activity_output
GROUP BY eao_id
HAVING COUNT(*) > 1;

SELECT 
   company_id,ea_id,act_output,year,
    COUNT(*) AS duplicate_count
FROM silver.envi_activity_output
GROUP BY 
   company_id,ea_id,act_output,year
HAVING COUNT(*) > 1; 

-- Check for unwanted whitespaces in all columns
-- Expectation: No Results
SELECT 
    'eao_id' AS column_name, 
    eao_id AS value
FROM silver.envi_activity_output
WHERE eao_id != TRIM(eao_id)
UNION ALL
SELECT 
    'company_id' AS column_name, 
    company_id AS value
FROM silver.envi_activity_output
WHERE company_id != TRIM(company_id)
UNION ALL
SELECT 
    'ea_id' AS column_name, 
    ea_id AS value
FROM silver.envi_activity_output
WHERE ea_id != TRIM(ea_id)
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, 
    unit_of_measurement AS value
FROM silver.envi_activity_output
WHERE unit_of_measurement != TRIM(unit_of_measurement);

-- Check for negative values in numeric columns
-- Expectation: No Results
SELECT 
    eao_id,
    act_output
FROM silver.envi_activity_output
WHERE act_output < 0;

SELECT 
    eao_id,
    year
FROM silver.envi_activity_output
WHERE year < 0;

-- Check for referential integrity
-- Expectation: No Results
SELECT 
    company_id 
FROM silver.envi_activity_output
WHERE company_id NOT IN (SELECT company_id FROM silver.envi_company_info);

SELECT 
    ea_id 
FROM silver.envi_activity_output
WHERE ea_id NOT IN (SELECT ea_id FROM silver.envi_activity);