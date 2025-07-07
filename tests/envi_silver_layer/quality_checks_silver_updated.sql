-- ======================================
-- QUALITY CHECKS FOR ENVIRONMENTAL SILVER LAYER
-- ======================================

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

-- Check referential integrity with company_main
-- Expectation: No Results
SELECT DISTINCT 
    ecp.company_id
FROM silver.envi_company_property ecp
LEFT JOIN ref.company_main cm ON ecp.company_id = cm.company_id
WHERE cm.company_id IS NULL;

-- Check for valid cp_type values
-- Expectation: Only expected values (Equipment, Vehicle, etc.)
SELECT 
    cp_type, 
    COUNT(*) AS count 
FROM silver.envi_company_property 
GROUP BY cp_type 
ORDER BY count DESC;

-- ======================================
-- QUALITY CHECKS FOR envi_water_abstraction
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for primary key
SELECT 
    'wa_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_water_abstraction 
WHERE wa_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_abstraction 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'volume' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_abstraction 
WHERE volume IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_abstraction 
WHERE unit_of_measurement IS NULL
UNION ALL
SELECT 
    'quarter' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_abstraction 
WHERE quarter IS NULL
UNION ALL
SELECT 
    'year' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_abstraction 
WHERE year IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    wa_id, 
    COUNT(*) AS duplicate_count 
FROM silver.envi_water_abstraction 
GROUP BY wa_id 
HAVING COUNT(*) > 1;

-- Check referential integrity with company_main
-- Expectation: No Results
SELECT DISTINCT 
    ewa.company_id
FROM silver.envi_water_abstraction ewa
LEFT JOIN ref.company_main cm ON ewa.company_id = cm.company_id
WHERE cm.company_id IS NULL;

-- Check for negative volumes
-- Expectation: No Results
SELECT 
    wa_id, 
    volume 
FROM silver.envi_water_abstraction 
WHERE volume < 0;

-- Check for reasonable volume ranges
-- Expectation: Review outliers
SELECT 
    MIN(volume) AS min_volume,
    MAX(volume) AS max_volume,
    AVG(volume) AS avg_volume,
    COUNT(*) AS total_records
FROM silver.envi_water_abstraction;

-- Check year range
-- Expectation: Years within reasonable range
SELECT 
    MIN(year) AS min_year,
    MAX(year) AS max_year,
    COUNT(DISTINCT year) AS unique_years
FROM silver.envi_water_abstraction;

-- ======================================
-- QUALITY CHECKS FOR envi_water_discharge
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for primary key
SELECT 
    'wd_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_water_discharge 
WHERE wd_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_discharge 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'volume' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_discharge 
WHERE volume IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_discharge 
WHERE unit_of_measurement IS NULL
UNION ALL
SELECT 
    'quarter' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_discharge 
WHERE quarter IS NULL
UNION ALL
SELECT 
    'year' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_discharge 
WHERE year IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    wd_id, 
    COUNT(*) AS duplicate_count 
FROM silver.envi_water_discharge 
GROUP BY wd_id 
HAVING COUNT(*) > 1;

-- Check referential integrity with company_main
-- Expectation: No Results
SELECT DISTINCT 
    ewd.company_id
FROM silver.envi_water_discharge ewd
LEFT JOIN ref.company_main cm ON ewd.company_id = cm.company_id
WHERE cm.company_id IS NULL;

-- Check for negative volumes
-- Expectation: No Results
SELECT 
    wd_id, 
    volume 
FROM silver.envi_water_discharge 
WHERE volume < 0;

-- ======================================
-- QUALITY CHECKS FOR envi_water_consumption
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for primary key
SELECT 
    'wc_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_water_consumption 
WHERE wc_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_consumption 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'volume' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_consumption 
WHERE volume IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_consumption 
WHERE unit_of_measurement IS NULL
UNION ALL
SELECT 
    'quarter' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_consumption 
WHERE quarter IS NULL
UNION ALL
SELECT 
    'year' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_water_consumption 
WHERE year IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    wc_id, 
    COUNT(*) AS duplicate_count 
FROM silver.envi_water_consumption 
GROUP BY wc_id 
HAVING COUNT(*) > 1;

-- Check referential integrity with company_main
-- Expectation: No Results
SELECT DISTINCT 
    ewc.company_id
FROM silver.envi_water_consumption ewc
LEFT JOIN ref.company_main cm ON ewc.company_id = cm.company_id
WHERE cm.company_id IS NULL;

-- Check for negative volumes
-- Expectation: No Results
SELECT 
    wc_id, 
    volume 
FROM silver.envi_water_consumption 
WHERE volume < 0;

-- ======================================
-- QUALITY CHECKS FOR envi_diesel_consumption
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for primary key
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
    'consumption' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_diesel_consumption 
WHERE consumption IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_diesel_consumption 
WHERE unit_of_measurement IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    dc_id, 
    COUNT(*) AS duplicate_count 
FROM silver.envi_diesel_consumption 
GROUP BY dc_id 
HAVING COUNT(*) > 1;

-- Check referential integrity with company_main
-- Expectation: No Results
SELECT DISTINCT 
    edc.company_id
FROM silver.envi_diesel_consumption edc
LEFT JOIN ref.company_main cm ON edc.company_id = cm.company_id
WHERE cm.company_id IS NULL;

-- Check referential integrity with company_property
-- Expectation: No Results
SELECT DISTINCT 
    edc.cp_id
FROM silver.envi_diesel_consumption edc
LEFT JOIN silver.envi_company_property ecp ON edc.cp_id = ecp.cp_id
WHERE ecp.cp_id IS NULL AND edc.cp_id IS NOT NULL;

-- Check for negative consumption
-- Expectation: No Results
SELECT 
    dc_id, 
    consumption 
FROM silver.envi_diesel_consumption 
WHERE consumption < 0;

-- ======================================
-- QUALITY CHECKS FOR envi_electric_consumption
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for primary key
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
    'source' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_electric_consumption 
WHERE source IS NULL
UNION ALL
SELECT 
    'consumption' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_electric_consumption 
WHERE consumption IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_electric_consumption 
WHERE unit_of_measurement IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    ec_id, 
    COUNT(*) AS duplicate_count 
FROM silver.envi_electric_consumption 
GROUP BY ec_id 
HAVING COUNT(*) > 1;

-- Check referential integrity with company_main
-- Expectation: No Results
SELECT DISTINCT 
    eec.company_id
FROM silver.envi_electric_consumption eec
LEFT JOIN ref.company_main cm ON eec.company_id = cm.company_id
WHERE cm.company_id IS NULL;

-- Check for negative consumption
-- Expectation: No Results
SELECT 
    ec_id, 
    consumption 
FROM silver.envi_electric_consumption 
WHERE consumption < 0;

-- Check source values
-- Expectation: Review unique sources
SELECT 
    source, 
    COUNT(*) AS count 
FROM silver.envi_electric_consumption 
GROUP BY source 
ORDER BY count DESC;

-- ======================================
-- QUALITY CHECKS FOR envi_non_hazard_waste
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for primary key
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
    'metrics' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_non_hazard_waste 
WHERE metrics IS NULL
UNION ALL
SELECT 
    'waste' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_non_hazard_waste 
WHERE waste IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_non_hazard_waste 
WHERE unit_of_measurement IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    nhw_id, 
    COUNT(*) AS duplicate_count 
FROM silver.envi_non_hazard_waste 
GROUP BY nhw_id 
HAVING COUNT(*) > 1;

-- Check referential integrity with company_main
-- Expectation: No Results
SELECT DISTINCT 
    enhw.company_id
FROM silver.envi_non_hazard_waste enhw
LEFT JOIN ref.company_main cm ON enhw.company_id = cm.company_id
WHERE cm.company_id IS NULL;

-- Check for negative waste
-- Expectation: No Results
SELECT 
    nhw_id, 
    waste 
FROM silver.envi_non_hazard_waste 
WHERE waste < 0;

-- ======================================
-- QUALITY CHECKS FOR envi_hazard_waste_generated
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for primary key
SELECT 
    'hwg_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_hazard_waste_generated 
WHERE hwg_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste_generated 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'metrics' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste_generated 
WHERE metrics IS NULL
UNION ALL
SELECT 
    'waste_generated' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste_generated 
WHERE waste_generated IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste_generated 
WHERE unit_of_measurement IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    hwg_id, 
    COUNT(*) AS duplicate_count 
FROM silver.envi_hazard_waste_generated 
GROUP BY hwg_id 
HAVING COUNT(*) > 1;

-- Check referential integrity with company_main
-- Expectation: No Results
SELECT DISTINCT 
    ehwg.company_id
FROM silver.envi_hazard_waste_generated ehwg
LEFT JOIN ref.company_main cm ON ehwg.company_id = cm.company_id
WHERE cm.company_id IS NULL;

-- Check for negative waste_generated
-- Expectation: No Results
SELECT 
    hwg_id, 
    waste_generated 
FROM silver.envi_hazard_waste_generated 
WHERE waste_generated < 0;

-- ======================================
-- QUALITY CHECKS FOR envi_hazard_waste_disposed
-- ======================================

-- Check for NULLs in columns
-- Expectation: No Results for primary key
SELECT 
    'hwd_id' AS column_name, COUNT(*) AS null_count 
FROM silver.envi_hazard_waste_disposed 
WHERE hwd_id IS NULL
UNION ALL
SELECT 
    'company_id' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste_disposed 
WHERE company_id IS NULL
UNION ALL
SELECT 
    'metrics' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste_disposed 
WHERE metrics IS NULL
UNION ALL
SELECT 
    'waste_disposed' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste_disposed 
WHERE waste_disposed IS NULL
UNION ALL
SELECT 
    'unit_of_measurement' AS column_name, COUNT(*) AS null_count  
FROM silver.envi_hazard_waste_disposed 
WHERE unit_of_measurement IS NULL;

-- Check for duplicates in primary key
-- Expectation: No Results
SELECT 
    hwd_id, 
    COUNT(*) AS duplicate_count 
FROM silver.envi_hazard_waste_disposed 
GROUP BY hwd_id 
HAVING COUNT(*) > 1;

-- Check referential integrity with company_main
-- Expectation: No Results
SELECT DISTINCT 
    ehwd.company_id
FROM silver.envi_hazard_waste_disposed ehwd
LEFT JOIN ref.company_main cm ON ehwd.company_id = cm.company_id
WHERE cm.company_id IS NULL;

-- Check for negative waste_disposed
-- Expectation: No Results
SELECT 
    hwd_id, 
    waste_disposed 
FROM silver.envi_hazard_waste_disposed 
WHERE waste_disposed < 0;

-- ======================================
-- CROSS-TABLE BUSINESS LOGIC CHECKS
-- ======================================

-- Check for companies with inconsistent data across tables
-- Expectation: Review for completeness
SELECT 
    cm.company_id,
    cm.company_name,
    COUNT(DISTINCT ecp.cp_id) AS properties_count,
    COUNT(DISTINCT ewa.wa_id) AS water_abstraction_count,
    COUNT(DISTINCT ewd.wd_id) AS water_discharge_count,
    COUNT(DISTINCT ewc.wc_id) AS water_consumption_count,
    COUNT(DISTINCT edc.dc_id) AS diesel_consumption_count,
    COUNT(DISTINCT eec.ec_id) AS electric_consumption_count
FROM ref.company_main cm
LEFT JOIN silver.envi_company_property ecp ON cm.company_id = ecp.company_id
LEFT JOIN silver.envi_water_abstraction ewa ON cm.company_id = ewa.company_id
LEFT JOIN silver.envi_water_discharge ewd ON cm.company_id = ewd.company_id
LEFT JOIN silver.envi_water_consumption ewc ON cm.company_id = ewc.company_id
LEFT JOIN silver.envi_diesel_consumption edc ON cm.company_id = edc.company_id
LEFT JOIN silver.envi_electric_consumption eec ON cm.company_id = eec.company_id
GROUP BY cm.company_id, cm.company_name
ORDER BY cm.company_id;

-- Check for potential data quality issues by year
-- Expectation: Review trends
SELECT 
    year,
    COUNT(DISTINCT ewa.company_id) AS companies_with_water_abstraction,
    COUNT(DISTINCT ewd.company_id) AS companies_with_water_discharge,
    COUNT(DISTINCT ewc.company_id) AS companies_with_water_consumption,
    COUNT(DISTINCT edc.company_id) AS companies_with_diesel_consumption,
    COUNT(DISTINCT eec.company_id) AS companies_with_electric_consumption
FROM silver.envi_water_abstraction ewa
FULL OUTER JOIN silver.envi_water_discharge ewd ON ewa.year = ewd.year
FULL OUTER JOIN silver.envi_water_consumption ewc ON ewa.year = ewc.year
FULL OUTER JOIN silver.envi_diesel_consumption edc ON ewa.year = edc.year
FULL OUTER JOIN silver.envi_electric_consumption eec ON ewa.year = eec.year
GROUP BY year
ORDER BY year;

-- Check for unit consistency
-- Expectation: Review for standardization
SELECT 
    'water_abstraction' AS table_name,
    unit_of_measurement,
    COUNT(*) AS count
FROM silver.envi_water_abstraction
GROUP BY unit_of_measurement
UNION ALL
SELECT 
    'water_discharge' AS table_name,
    unit_of_measurement,
    COUNT(*) AS count
FROM silver.envi_water_discharge
GROUP BY unit_of_measurement
UNION ALL
SELECT 
    'water_consumption' AS table_name,
    unit_of_measurement,
    COUNT(*) AS count
FROM silver.envi_water_consumption
GROUP BY unit_of_measurement
UNION ALL
SELECT 
    'diesel_consumption' AS table_name,
    unit_of_measurement,
    COUNT(*) AS count
FROM silver.envi_diesel_consumption
GROUP BY unit_of_measurement
UNION ALL
SELECT 
    'electric_consumption' AS table_name,
    unit_of_measurement,
    COUNT(*) AS count
FROM silver.envi_electric_consumption
GROUP BY unit_of_measurement
UNION ALL
SELECT 
    'non_hazard_waste' AS table_name,
    unit_of_measurement,
    COUNT(*) AS count
FROM silver.envi_non_hazard_waste
GROUP BY unit_of_measurement
UNION ALL
SELECT 
    'hazard_waste_generated' AS table_name,
    unit_of_measurement,
    COUNT(*) AS count
FROM silver.envi_hazard_waste_generated
GROUP BY unit_of_measurement
UNION ALL
SELECT 
    'hazard_waste_disposed' AS table_name,
    unit_of_measurement,
    COUNT(*) AS count
FROM silver.envi_hazard_waste_disposed
GROUP BY unit_of_measurement
ORDER BY table_name, unit_of_measurement;

-- ======================================
-- SUMMARY STATISTICS
-- ======================================

-- Record counts by table
SELECT 
    'envi_company_property' AS table_name,
    COUNT(*) AS record_count
FROM silver.envi_company_property
UNION ALL
SELECT 
    'envi_water_abstraction' AS table_name,
    COUNT(*) AS record_count
FROM silver.envi_water_abstraction
UNION ALL
SELECT 
    'envi_water_discharge' AS table_name,
    COUNT(*) AS record_count
FROM silver.envi_water_discharge
UNION ALL
SELECT 
    'envi_water_consumption' AS table_name,
    COUNT(*) AS record_count
FROM silver.envi_water_consumption
UNION ALL
SELECT 
    'envi_diesel_consumption' AS table_name,
    COUNT(*) AS record_count
FROM silver.envi_diesel_consumption
UNION ALL
SELECT 
    'envi_electric_consumption' AS table_name,
    COUNT(*) AS record_count
FROM silver.envi_electric_consumption
UNION ALL
SELECT 
    'envi_non_hazard_waste' AS table_name,
    COUNT(*) AS record_count
FROM silver.envi_non_hazard_waste
UNION ALL
SELECT 
    'envi_hazard_waste_generated' AS table_name,
    COUNT(*) AS record_count
FROM silver.envi_hazard_waste_generated
UNION ALL
SELECT 
    'envi_hazard_waste_disposed' AS table_name,
    COUNT(*) AS record_count
FROM silver.envi_hazard_waste_disposed
ORDER BY table_name;
