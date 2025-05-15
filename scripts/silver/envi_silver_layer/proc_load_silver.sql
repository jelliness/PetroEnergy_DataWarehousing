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

-- Create or replace the master procedure
CREATE OR REPLACE PROCEDURE silver.load_envi_silver()
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
        
        -- Loading silver.envi_company_property
        start_time := CLOCK_TIMESTAMP();
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
        FROM bronze.envi_company_property
        ON CONFLICT (cp_id)
        DO UPDATE SET
            company_id = EXCLUDED.company_id,
            cp_name = EXCLUDED.cp_name,
            cp_type = EXCLUDED.cp_type;
        
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_natural_sources
        start_time := CLOCK_TIMESTAMP();
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
        FROM bronze.envi_natural_sources
        ON CONFLICT (ns_id)
        DO UPDATE SET
            company_id = EXCLUDED.company_id,
            ns_name = EXCLUDED.ns_name;

        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_water_withdrawal
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Inserting Data Into: silver.envi_water_withdrawal';
        INSERT INTO silver.envi_water_withdrawal (
            ww_id,
            company_id,
            ns_id,
            volume,
            unit_of_measurement,
            year,
            month,
            quarter
        )
        SELECT
            TRIM(ww_id),
            TRIM(company_id),
            TRIM(ns_id),
            CASE
                WHEN volume < 0 THEN 0  -- Handle negative values
                ELSE volume
            END AS volume,
            TRIM(unit_of_measurement),
            year,
            month,
            CASE 
		        WHEN LOWER(TRIM(month)) IN ('january', 'february', 'march') THEN 'Q1'
		        WHEN LOWER(TRIM(month)) IN ('april', 'may', 'june') THEN 'Q2'
		        WHEN LOWER(TRIM(month)) IN ('july', 'august', 'september') THEN 'Q3'
		        WHEN LOWER(TRIM(month)) IN ('october', 'november', 'december') THEN 'Q4'
		        ELSE 'Unknown'
		    END AS quarter
        FROM bronze.envi_water_withdrawal
        ON CONFLICT (ww_id)
        DO UPDATE SET
            company_id = EXCLUDED.company_id,
            ns_id = EXCLUDED.ns_id,
            volume = CASE
                WHEN EXCLUDED.volume < 0 THEN 0  -- Handle negative values
                ELSE EXCLUDED.volume
            END,
            unit_of_measurement = EXCLUDED.unit_of_measurement,
            year = EXCLUDED.year,
            month = EXCLUDED.month,
            quarter = CASE 
		        WHEN LOWER(TRIM(EXCLUDED.month)) IN ('january', 'february', 'march') THEN 'Q1'
		        WHEN LOWER(TRIM(EXCLUDED.month)) IN ('april', 'may', 'june') THEN 'Q2'
		        WHEN LOWER(TRIM(EXCLUDED.month)) IN ('july', 'august', 'september') THEN 'Q3'
		        WHEN LOWER(TRIM(EXCLUDED.month)) IN ('october', 'november', 'december') THEN 'Q4'
		        ELSE 'Unknown'
		    END;

        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_diesel_consumption
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Inserting Data Into: silver.envi_diesel_consumption';
		INSERT INTO silver.envi_diesel_consumption (
		    dc_id,
		    company_id,
		    cp_id,
		    unit_of_measurement,
		    consumption,
		    month,
		    year,
		    quarter,
		    date
		)
		SELECT
		    TRIM(dc_id),
		    TRIM(company_id),
		    TRIM(cp_id),
		    TRIM(unit_of_measurement),
		    CASE
		        WHEN consumption < 0 THEN 0 -- Handle negative values
		        ELSE consumption
		    END AS consumption,
		    TO_CHAR(date, 'Month') AS month,
		    EXTRACT(YEAR FROM date)::INT AS year,
		    CASE
		        WHEN EXTRACT(MONTH FROM date) BETWEEN 1 AND 3 THEN 'Q1'
		        WHEN EXTRACT(MONTH FROM date) BETWEEN 4 AND 6 THEN 'Q2'
		        WHEN EXTRACT(MONTH FROM date) BETWEEN 7 AND 9 THEN 'Q3'
		        WHEN EXTRACT(MONTH FROM date) BETWEEN 10 AND 12 THEN 'Q4'
		    END AS quarter,
		    date
		FROM bronze.envi_diesel_consumption
		ON CONFLICT (dc_id) DO UPDATE SET
		    company_id = EXCLUDED.company_id,
		    cp_id = EXCLUDED.cp_id,
		    unit_of_measurement = EXCLUDED.unit_of_measurement,
		    consumption = CASE
		        WHEN EXCLUDED.consumption < 0 THEN 0 -- Handle negative values
		        ELSE EXCLUDED.consumption
		    END,
		    month = EXCLUDED.month,
		    year = EXCLUDED.year,
		    quarter = CASE
		        WHEN EXTRACT(MONTH FROM EXCLUDED.date) BETWEEN 1 AND 3 THEN 'Q1'
		        WHEN EXTRACT(MONTH FROM EXCLUDED.date) BETWEEN 4 AND 6 THEN 'Q2'
		        WHEN EXTRACT(MONTH FROM EXCLUDED.date) BETWEEN 7 AND 9 THEN 'Q3'
		        WHEN EXTRACT(MONTH FROM EXCLUDED.date) BETWEEN 10 AND 12 THEN 'Q4'
			END,
		    date = EXCLUDED.date,
		    updated_at = CURRENT_TIMESTAMP;

        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_electric_consumption
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Inserting Data Into: silver.envi_electric_consumption';
        -- Loading silver.envi_electric_consumption
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
		FROM bronze.envi_electric_consumption
		ON CONFLICT (ec_id)
		DO UPDATE SET
		    company_id = EXCLUDED.company_id,
		    unit_of_measurement = EXCLUDED.unit_of_measurement,
		    consumption = CASE
		        WHEN EXCLUDED.consumption < 0 THEN 0  -- Handle negative values
		        ELSE EXCLUDED.consumption
		    END,
		    quarter = EXCLUDED.quarter,
		    year = EXCLUDED.year;

        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_non_hazard_waste
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Inserting Data Into: silver.envi_non_hazard_waste';
        INSERT INTO silver.envi_non_hazard_waste (
            nhw_id,
            company_id,
            waste_source,
            metrics,
            unit_of_measurement,
            waste,
            month,
            year,
            quarter
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
            year,
            CASE 
		        WHEN LOWER(TRIM(month)) IN ('january', 'february', 'march') THEN 'Q1'
		        WHEN LOWER(TRIM(month)) IN ('april', 'may', 'june') THEN 'Q2'
		        WHEN LOWER(TRIM(month)) IN ('july', 'august', 'september') THEN 'Q3'
		        WHEN LOWER(TRIM(month)) IN ('october', 'november', 'december') THEN 'Q4'
		        ELSE 'Unknown'
		    END AS quarter
        FROM bronze.envi_non_hazard_waste
        ON CONFLICT (nhw_id)
        DO UPDATE SET
            company_id = EXCLUDED.company_id,
            waste_source = EXCLUDED.waste_source,
            metrics = EXCLUDED.metrics,
            unit_of_measurement = EXCLUDED.unit_of_measurement,
            waste = CASE
                WHEN EXCLUDED.waste < 0 THEN 0  -- Handle negative values
                ELSE EXCLUDED.waste
            END,
            month = EXCLUDED.month,
            year = EXCLUDED.year,
            quarter = CASE 
		        WHEN LOWER(TRIM(EXCLUDED.month)) IN ('january', 'february', 'march') THEN 'Q1'
		        WHEN LOWER(TRIM(EXCLUDED.month)) IN ('april', 'may', 'june') THEN 'Q2'
		        WHEN LOWER(TRIM(EXCLUDED.month)) IN ('july', 'august', 'september') THEN 'Q3'
		        WHEN LOWER(TRIM(EXCLUDED.month)) IN ('october', 'november', 'december') THEN 'Q4'
		        ELSE 'Unknown'
		    END;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';
        
        -- Loading silver.envi_hazard_waste_generated
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Inserting Data Into: silver.envi_hazard_waste_generated';
        INSERT INTO silver.envi_hazard_waste_generated (
            hwg_id,
            company_id,
            metrics,
            unit_of_measurement,
            waste_generated,
            quarter,
            year
        )
        SELECT
            TRIM(hwg_id),
            TRIM(company_id),
            TRIM(metrics),
            TRIM(unit_of_measurement),
            CASE
                WHEN waste_generated < 0 THEN 0  -- Handle negative values
                ELSE waste_generated
            END AS waste_generated,
			TRIM(quarter),
            year
        FROM bronze.envi_hazard_waste_generated
        ON CONFLICT (hwg_id)
        DO UPDATE SET
            company_id = EXCLUDED.company_id,
            metrics = EXCLUDED.metrics,
            unit_of_measurement = EXCLUDED.unit_of_measurement,
            waste_generated = CASE
                WHEN EXCLUDED.waste_generated < 0 THEN 0  -- Handle negative values
                ELSE EXCLUDED.waste_generated
            END,
			quarter = EXCLUDED.quarter,
            year = EXCLUDED.year;

        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';


        -- Loading silver.envi_hazard_waste_disposed
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Inserting Data Into: silver.envi_hazard_waste_disposed';
        INSERT INTO silver.envi_hazard_waste_disposed (
            hwd_id,
            company_id,
            metrics,
            unit_of_measurement,
            waste_disposed,
            year
        )
        SELECT
            TRIM(hwd_id),
            TRIM(company_id),
            TRIM(metrics),
            TRIM(unit_of_measurement),
            CASE
                WHEN waste_disposed < 0 THEN 0  -- Handle negative values
                ELSE waste_disposed
            END AS waste_disposed,
            year
        FROM bronze.envi_hazard_waste_disposed
        ON CONFLICT (hwd_id)
        DO UPDATE SET
            company_id = EXCLUDED.company_id,
            metrics = EXCLUDED.metrics,
            unit_of_measurement = EXCLUDED.unit_of_measurement,
            waste_disposed = CASE
                WHEN EXCLUDED.waste_disposed < 0 THEN 0  -- Handle negative values
                ELSE EXCLUDED.waste_disposed
            END,
            year = EXCLUDED.year;
            
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

CALL silver.load_envi_silver()