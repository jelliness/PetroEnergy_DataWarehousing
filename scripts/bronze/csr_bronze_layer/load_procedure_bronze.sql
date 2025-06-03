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

    BEGIN

        -- csr_programs
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading CSR Programs Data...';
        RAISE NOTICE '------------------------------------------------';

        start_time := CURRENT_TIMESTAMP;

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
        FROM bronze.csr_programs
        ON CONFLICT (program_id) DO UPDATE
        SET
            program_name = EXCLUDED.program_name,
            date_updated = CURRENT_TIMESTAMP;

        end_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Load Duration (csr_programs): % seconds', EXTRACT(EPOCH FROM end_time - start_time);
        RAISE NOTICE '-----------------';

        -- csr_projects
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading CSR Projects Data...';
        RAISE NOTICE '------------------------------------------------';

        start_time := CURRENT_TIMESTAMP;

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
        FROM bronze.csr_projects
        ON CONFLICT (project_id) DO UPDATE
        SET
            program_id = EXCLUDED.program_id,
            project_name = EXCLUDED.project_name,
            project_metrics = EXCLUDED.project_metrics,
            date_updated = CURRENT_TIMESTAMP;

        end_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Load Duration (csr_projects): % seconds', EXTRACT(EPOCH FROM end_time - start_time);
        RAISE NOTICE '-----------------';

        -- csr_activity
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading CSR Activity Data...';
        RAISE NOTICE '------------------------------------------------';

        start_time := CURRENT_TIMESTAMP;

        INSERT INTO silver.csr_activity (
            csr_id,
            company_id,
            project_id,
            project_year,
            csr_report,
            project_expenses,
            date_created,
            date_updated
        )
        SELECT
            COALESCE(NULLIF(TRIM(csr_id), ''), 'Not Available'),
            COALESCE(NULLIF(TRIM(company_id), ''), 'Not Available'),
            COALESCE(NULLIF(TRIM(project_id), ''), 'Not Available'),
            CASE
                WHEN project_year IS NULL THEN NULL
                ELSE project_year
            END,
            CASE
                WHEN csr_report IS NULL THEN NULL
                ELSE csr_report
            END,
			CASE
                WHEN project_expenses IS NULL THEN NULL
                ELSE project_expenses
            END,
            NOW(), NOW()
        FROM bronze.csr_activity
        ON CONFLICT (csr_id) DO UPDATE
        SET
            company_id = EXCLUDED.company_id,
            project_id = EXCLUDED.project_id,
            project_year = EXCLUDED.project_year,
            csr_report = EXCLUDED.csr_report,
            project_expenses = EXCLUDED.project_expenses,
            date_updated = CURRENT_TIMESTAMP;

        end_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Load Duration (csr_activity): % seconds', EXTRACT(EPOCH FROM end_time - start_time);
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
END;
$$;

CALL silver.load_csr_silver()