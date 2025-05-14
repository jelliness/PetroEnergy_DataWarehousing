/*
===============================================================================
Stored Procedure: Insert Trimmed Data from Bronze to Silver
===============================================================================
Script Purpose:
    This procedure transfers data from bronze tables to silver tables,
    trimming string values and handling data transformation.
    
    The procedure follows the standard ETL pattern with detailed logging
    of execution times and error handling.
===============================================================================
*/

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS silver;

-- Create or replace the master procedure
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';
    
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading Environmental Tables';
    RAISE NOTICE '------------------------------------------------';
    
    BEGIN

        -- Loading silver.envi_company_info
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.envi_company_info';
        TRUNCATE TABLE silver.envi_company_info;
        RAISE NOTICE '>> Inserting Data Into: silver.envi_company_info';
        INSERT INTO silver.envi_company_info (
            company_id,
            company_name,
            resources,
            site_name,
            site_address,
            city_town,
            province,
            zip
        )
        SELECT
            TRIM(company_id),
            TRIM(company_name),
            TRIM(resources),
            TRIM(site_name),
            TRIM(site_address),
            TRIM(city_town),
            TRIM(province),
            TRIM(zip)
        FROM bronze.envi_company_info;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_company_property
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.envi_company_property';
        TRUNCATE TABLE silver.envi_company_property;
        RAISE NOTICE '>> Inserting Data Into: silver.envi_company_property';
        INSERT INTO silver.envi_company_property (
            cp_id,
            company_id,
            cp_name,
            cp_type
        )
        SELECT
            TRIM(cp_id),
            TRIM(company_id),
            TRIM(cp_name),
			TRIM(cp_type)
        FROM bronze.envi_company_property;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_natural_sources
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.envi_natural_sources';
        TRUNCATE TABLE silver.envi_natural_sources;
        RAISE NOTICE '>> Inserting Data Into: silver.envi_natural_sources';
        INSERT INTO silver.envi_natural_sources (
            ns_id,
            company_id,
            ns_name
        )
        SELECT
            TRIM(ns_id),
            TRIM(company_id),
            TRIM(ns_name)
        FROM bronze.envi_natural_sources;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_water_withdrawal
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.envi_water_withdrawal';
        TRUNCATE TABLE silver.envi_water_withdrawal;
        RAISE NOTICE '>> Inserting Data Into: silver.envi_water_withdrawal';
        INSERT INTO silver.envi_water_withdrawal (
            ww_id,
            company_id,
            year,
            month,
            ns_id,
            volume,
            unit_of_measurement
        )
        SELECT
            TRIM(ww_id),
            TRIM(company_id),
            year,
            month,
            TRIM(ns_id),
            CASE
                WHEN volume < 0 THEN 0  -- Handle negative values
                ELSE volume
            END AS volume,
            TRIM(unit_of_measurement)
        FROM bronze.envi_water_withdrawal;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_diesel_consumption
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.envi_diesel_consumption';
        TRUNCATE TABLE silver.envi_diesel_consumption;
        RAISE NOTICE '>> Inserting Data Into: silver.envi_diesel_consumption';
        INSERT INTO silver.envi_diesel_consumption (
            dc_id,
            company_id,
            cp_id,
            unit_of_measurement,
            consumption,
            date,
            month
        )
        SELECT
            TRIM(dc_id),
            TRIM(company_id),
            TRIM(cp_id),
            TRIM(unit_of_measurement),
            CASE
                WHEN consumption < 0 THEN 0  -- Handle negative values
                ELSE consumption
            END AS consumption,
            date,
            month
        FROM bronze.envi_diesel_consumption;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_electric_consumption
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.envi_electric_consumption';
        TRUNCATE TABLE silver.envi_electric_consumption;
        RAISE NOTICE '>> Inserting Data Into: silver.envi_electric_consumption';
        INSERT INTO silver.envi_electric_consumption (
            ec_id,
            company_id,
            unit_of_measurement,
            consumption,
            quarter,
            year
        )
        SELECT
            TRIM(ec_id),
            TRIM(company_id),
            TRIM(unit_of_measurement),
            CASE
                WHEN consumption < 0 THEN 0  -- Handle negative values
                ELSE consumption
            END AS consumption,
            TRIM(quarter),
            year
        FROM bronze.envi_electric_consumption;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_power_generation
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.envi_power_generation';
        TRUNCATE TABLE silver.envi_power_generation;
        RAISE NOTICE '>> Inserting Data Into: silver.envi_power_generation';
        INSERT INTO silver.envi_power_generation (
            pg_id,
            company_id,
            unit_of_measurement,
            generation,
            quarter,
            year
        )
        SELECT
            TRIM(pg_id),
            TRIM(company_id),
            TRIM(unit_of_measurement),
            CASE
                WHEN generation < 0 THEN 0  -- Handle negative values
                ELSE generation
            END AS generation,
            TRIM(Quarter),
            year
        FROM bronze.envi_power_generation;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_non_hazard_waste
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.envi_non_hazard_waste';
        TRUNCATE TABLE silver.envi_non_hazard_waste;
        RAISE NOTICE '>> Inserting Data Into: silver.envi_non_hazard_waste';
        INSERT INTO silver.envi_non_hazard_waste (
            nhw_id,
            company_id,
            waste_source,
            metrics,
            unit_of_measurement,
            waste,
            month,
            year
        )
        SELECT
            TRIM(nhw_id),
            TRIM(company_id),
            TRIM(waste_source),
            TRIM(metrics),
            TRIM(unit_of_measurement),
            CASE
                WHEN waste < 0 THEN 0  -- Handle negative values
                ELSE waste
            END AS waste,
            month,
            year
        FROM bronze.envi_non_hazard_waste;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_hazard_waste
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.envi_hazard_waste';
        TRUNCATE TABLE silver.envi_hazard_waste;
        RAISE NOTICE '>> Inserting Data Into: silver.envi_hazard_waste';
        INSERT INTO silver.envi_hazard_waste (
            hw_id,
            company_id,
            metrics,
            unit_of_measurement,
            waste,
            quarter,
            year
        )
        SELECT
            TRIM(hw_id),
            TRIM(company_id),
            TRIM(metrics),
            TRIM(unit_of_measurement),
            CASE
                WHEN waste < 0 THEN 0  -- Handle negative values
                ELSE waste
            END AS waste,
            TRIM(quarter),
            year
        FROM bronze.envi_hazard_waste;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_activity
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.envi_activity';
        TRUNCATE TABLE silver.envi_activity;
        RAISE NOTICE '>> Inserting Data Into: silver.envi_activity';
        INSERT INTO silver.envi_activity (
            ea_id,
            metrics,
            company_id,
            envi_act_name
        )
        SELECT
            TRIM(ea_id),
            TRIM(metrics),
            TRIM(company_id),
            TRIM(envi_act_name)
        FROM bronze.envi_activity;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_activity_output
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.envi_activity_output';
        TRUNCATE TABLE silver.envi_activity_output;
        RAISE NOTICE '>> Inserting Data Into: silver.envi_activity_output';
        INSERT INTO silver.envi_activity_output (
            eao_id,
            company_id,
            ea_id,
            unit_of_measurement,
            act_output,
            year
        )
        SELECT
            TRIM(eao_id),
            TRIM(company_id),
            TRIM(ea_id),
            TRIM(unit_of_measurement),
            CASE
                WHEN act_output < 0 THEN 0  -- Handle negative values
                ELSE act_output
            END AS act_output,
            year
        FROM bronze.envi_activity_output;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        batch_end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'Loading Silver Layer is Completed';
        RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
        RAISE NOTICE '==========================================';
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '==========================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE 'Error Code: %', SQLSTATE;
            RAISE NOTICE '==========================================';
    END;
END;
$$;

CALL silver.load_silver()