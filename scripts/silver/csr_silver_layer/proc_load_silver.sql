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
    null_count INT;
BEGIN
    batch_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading Silver Layer Data...';
    RAISE NOTICE '================================';

    -- csr_company
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Company Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.csr_company;

    INSERT INTO silver.csr_company (
        company_id,
        company_name,
        resources,
        date_created,
        date_updated
    )
    SELECT
        COALESCE(NULLIF(TRIM(company_id), ''), 'Not Available'),
        COALESCE(NULLIF(TRIM(company_name), ''), 'Not Available'),
        COALESCE(NULLIF(TRIM(resources), ''), 'Not Available'),
        NOW(), NOW()
    FROM bronze.csr_company;

    SELECT COUNT(*) INTO null_count
    FROM bronze.csr_company
    WHERE company_id IS NULL OR company_id = ''
       OR company_name IS NULL OR company_name = ''
       OR resources IS NULL OR resources = '';

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration (csr_company): % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Null Values Found: %', null_count;
    RAISE NOTICE '-----------------';

    -- csr_programs
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Programs Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.csr_programs;

    INSERT INTO silver.csr_programs (
        program_id,
        program_name,
        date_created,
        date_updated
    )
    SELECT
        COALESCE(NULLIF(TRIM(program_id), ''), 'Not Available'),
        COALESCE(NULLIF(TRIM(program_name), ''), 'Not Available'),
        NOW(), NOW()
    FROM bronze.csr_programs;

    SELECT COUNT(*) INTO null_count
    FROM bronze.csr_programs
    WHERE program_id IS NULL OR program_id = ''
       OR program_name IS NULL OR program_name = '';

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration (csr_programs): % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Null Values Found: %', null_count;
    RAISE NOTICE '-----------------';

    -- csr_projects
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Projects Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.csr_projects;

    INSERT INTO silver.csr_projects (
        project_id,
        program_id,
        project_name,
        project_metrics,
        date_created,
        date_updated
    )
    SELECT
        COALESCE(NULLIF(TRIM(project_id), ''), 'Not Available'),
        COALESCE(NULLIF(TRIM(program_id), ''), 'Not Available'),
        COALESCE(NULLIF(TRIM(project_name), ''), 'Not Available'),
        COALESCE(NULLIF(TRIM(project_metrics), ''), 'Not Available'),
        NOW(), NOW()
    FROM bronze.csr_projects;

    SELECT COUNT(*) INTO null_count
    FROM bronze.csr_projects
    WHERE project_id IS NULL OR project_id = ''
       OR program_id IS NULL OR program_id = ''
       OR project_name IS NULL OR project_name = ''
       OR project_metrics IS NULL OR project_metrics = '';

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration (csr_projects): % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Null Values Found: %', null_count;
    RAISE NOTICE '-----------------';

    -- csr_per_company
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Per Company Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.csr_per_company;

    INSERT INTO silver.csr_per_company (
        inv_id,
        company_id,
        program_id,
        program_investment,
        date_created,
        date_updated
    )
    SELECT
        COALESCE(NULLIF(TRIM(inv_id), ''), 'Not Available'),
        COALESCE(NULLIF(TRIM(company_id), ''), 'Not Available'),
        COALESCE(NULLIF(TRIM(program_id), ''), 'Not Available'),
        CASE
            WHEN program_investment IS NULL THEN NULL
            ELSE program_investment
        END,
        NOW(), NOW()
    FROM bronze.csr_per_company;

    SELECT COUNT(*) INTO null_count
    FROM bronze.csr_per_company
    WHERE inv_id IS NULL OR inv_id = ''
       OR company_id IS NULL OR company_id = ''
       OR program_id IS NULL OR program_id = ''
       OR program_investment IS NULL;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration (csr_per_company): % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Null Values Found: %', null_count;
    RAISE NOTICE '-----------------';

    -- csr_activity
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Activity Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.csr_activity;

    INSERT INTO silver.csr_activity (
        csr_id,
        company_id,
        project_id,
        ac_year,
        csr_report,
        date_created,
        date_updated
    )
    SELECT
        COALESCE(NULLIF(TRIM(csr_id), ''), 'Not Available'),
        COALESCE(NULLIF(TRIM(company_id), ''), 'Not Available'),
        COALESCE(NULLIF(TRIM(project_id), ''), 'Not Available'),
        CASE
            WHEN ac_year ~ '^\d{4}$' THEN ac_year
            ELSE 'Not Available'
        END,
        CASE
            WHEN csr_report IS NULL THEN NULL
            ELSE csr_report
        END,
        NOW(), NOW()
    FROM bronze.csr_activity;

    SELECT COUNT(*) INTO null_count
    FROM bronze.csr_activity
    WHERE csr_id IS NULL OR csr_id = ''
       OR company_id IS NULL OR company_id = ''
       OR project_id IS NULL OR project_id = ''
       OR ac_year IS NULL OR NOT ac_year ~ '^\d{4}$'
       OR csr_report IS NULL;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration (csr_activity): % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '>> Null Values Found: %', null_count;
    RAISE NOTICE '-----------------';

    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading CSR Silver Layer is Completed';
    RAISE NOTICE '     - Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading silver data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$$;
