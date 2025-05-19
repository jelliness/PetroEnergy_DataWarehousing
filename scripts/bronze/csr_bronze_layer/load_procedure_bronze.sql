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
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading Bronze Layer Data...';
    RAISE NOTICE '================================';

    -- csr_company
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Company Information Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: bronze.csr_company...';
    TRUNCATE TABLE bronze.csr_company;
    RAISE NOTICE '>> Bulk inserting data into bronze.csr_company...';

    COPY bronze.csr_company
    FROM 'C:\Users\CJ Dumlao\Documents\GitHub\PetroEnergy_DataWarehousing\datasets\source_csr\csr_company.csv'
    DELIMITER ',' CSV HEADER;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    -- csr_programs
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSR Programs Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: bronze.csr_programs...';
    TRUNCATE TABLE bronze.csr_programs;
    RAISE NOTICE '>> Bulk inserting data into bronze.csr_programs...';

    COPY bronze.csr_programs
    FROM 'C:\Users\CJ Dumlao\Documents\GitHub\PetroEnergy_DataWarehousing\datasets\source_csr\csr_programs.csv'
    DELIMITER ',' CSV HEADER;

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

    COPY bronze.csr_projects
    FROM 'C:\Users\CJ Dumlao\Documents\GitHub\PetroEnergy_DataWarehousing\datasets\source_csr\csr_projects.csv'
    DELIMITER ',' CSV HEADER;

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

    COPY bronze.csr_activity
    FROM 'C:\Users\CJ Dumlao\Documents\GitHub\PetroEnergy_DataWarehousing\datasets\source_csr\csr_activity.csv'
    DELIMITER ',' CSV HEADER;

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
