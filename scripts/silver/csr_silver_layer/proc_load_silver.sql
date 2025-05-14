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
CREATE OR REPLACE PROCEDURE silver.load_csr_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;

BEGIN

    -- Start time for the batch operation
    batch_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading Silver Layer Data...';
    RAISE NOTICE '================================';

    -- Load CSR Accomplishments Data
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Accomplishments Data...';
    RAISE NOTICE '------------------------------------------------';

    -- Start time for this specific operation
    start_time := CURRENT_TIMESTAMP;

    -- Truncate the existing table in the silver layer
    RAISE NOTICE '>> Truncating table: silver.csr_accomplishments...';
    TRUNCATE TABLE silver.csr_accomplishments;

    -- Load the transformed data into the silver layer table
    RAISE NOTICE '>> Inserting transformed data into silver.csr_accomplishments...';
    INSERT INTO silver.csr_accomplishments (
        ac_id,
        company_id,
        ac_year,
        program,
        csr_report
    )
    SELECT
        -- Ensure consistent types for all COALESCE operations
        COALESCE(NULLIF(TRIM(ac_id), ''), 'UNKNOWN') AS ac_id,
        COALESCE(NULLIF(TRIM(company_id), ''), 'UNKNOWN') AS company_id,
        -- Ensure ac_year is treated as an integer, inserting NULL for null/empty values
        CASE 
            WHEN TRIM(ac_year::TEXT) = '' THEN NULL
            ELSE ac_year::INTEGER
        END AS ac_year,
        COALESCE(NULLIF(TRIM(program), ''), 'UNKNOWN') AS program,
        CASE 
            -- Ensure csr_report is treated as an integer, defaulting to 0 for non-numeric or negative values
            WHEN TRIM(csr_report::TEXT) ~ '^\d+$' THEN 
                GREATEST(csr_report::INTEGER, 0)
            ELSE 
                0
        END AS csr_report
    FROM bronze.csr_accomplishments;

    -- End time for this specific operation
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------' ;

    -- End time for the batch operation
    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading CSR Silver Layer is Completed';
    RAISE NOTICE '     - Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$$;
