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
    batch_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading Silver Layer Data...';
    RAISE NOTICE '================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Accomplishments Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;

    RAISE NOTICE '>> Truncating table: silver.csr_accomplishments...';
    TRUNCATE TABLE silver.csr_accomplishments;

    RAISE NOTICE '>> Inserting transformed data into silver.csr_accomplishments...';
    INSERT INTO silver.csr_accomplishments (
        ac_id,
        company_id,
        ac_year,
        csr_program,
        csr_report,
        date_created,
        date_updated
    )
    SELECT
        COALESCE(NULLIF(TRIM(ac_id), ''), 'Not Available') AS ac_id,
        COALESCE(NULLIF(TRIM(company_id), ''), 'Not Available') AS company_id,
        CASE 
            WHEN TRIM(ac_year::TEXT) ~ '^\d+$' THEN TRIM(ac_year::TEXT)
            ELSE 'Not Available'
        END AS ac_year,
        COALESCE(NULLIF(TRIM(csr_program), ''), 'Not Available') AS csr_program,
        CASE 
            WHEN TRIM(csr_report::TEXT) ~ '^\d+$' THEN TRIM(csr_report::TEXT)
            ELSE 'Not Available'
        END AS csr_report,
        NOW() AS date_created,
        NOW() AS date_updated
    FROM bronze.csr_accomplishments;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------' ;

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
