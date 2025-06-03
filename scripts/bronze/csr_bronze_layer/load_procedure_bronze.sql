-- This script is used to create a stored procedure that performs the following tasks:

-- UTILIZATION: 'EXEC bronze.load_csr_bronze;'

-- This script is used to create a stored procedure that performs the following tasks:
-- 1. Truncates the existing tables in the bronze layer.
-- 2. Bulk inserts data from CSV files into the bronze layer tables.
-- 3. The script includes the necessary configurations for the bulk insert operation.
-- 4. The script also includes error handling to catch any errors that occur during the execution of the procedure.
-- 5. The script prints the start and end time of each operation, as well as the total duration of the batch operation.

CREATE OR REPLACE PROCEDURE bronze.load_csr_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    local_file_path TEXT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    -- Set the local file path here for easy directory changes
    local_file_path := 'C:\Users\CJ Dumlao\Documents\GitHub\PetroEnergy_DataWarehousing\datasets\source_csr';
    
    batch_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading Bronze Layer Data...';
    RAISE NOTICE '================================';

    -- csr_programs
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Programs Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: bronze.csr_programs...';
    TRUNCATE TABLE bronze.csr_programs;
    RAISE NOTICE '>> Bulk inserting data into bronze.csr_programs...';

    EXECUTE format(
        'COPY bronze.csr_programs FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '\csr_programs.csv'
    );

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    -- csr_projects
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Projects Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: bronze.csr_projects...';
    TRUNCATE TABLE bronze.csr_projects;
    RAISE NOTICE '>> Bulk inserting data into bronze.csr_projects...';

    EXECUTE format(
        'COPY bronze.csr_projects FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '\csr_projects.csv'
    );

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    -- csr_activity
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Activity Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: bronze.csr_activity...';
    TRUNCATE TABLE bronze.csr_activity;
    RAISE NOTICE '>> Bulk inserting data into bronze.csr_activity...';

    EXECUTE format(
        'COPY bronze.csr_activity FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '\csr_activity.csv'
    );

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading CSR Bronze Layer is Completed';
    RAISE NOTICE '     - Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$$;
