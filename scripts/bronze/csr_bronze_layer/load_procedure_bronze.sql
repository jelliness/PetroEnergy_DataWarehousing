-- This script is used to create a stored procedure that performs the following tasks:

-- UTILIZATION: 'EXEC bronze.load_csr_bronze;'

-- This script is used to create a stored procedure that performs the following tasks:
-- 1. Truncates the existing tables in the bronze layer.
-- 2. Bulk inserts data from CSV files into the bronze layer tables.
-- 3. The script includes the necessary configurations for the bulk insert operation.
-- 4. The script also includes error handling to catch any errors that occur during the execution of the procedure.
-- 5. The script prints the start and end time of each operation, as well as the total duration of the batch operation.

CALL bronze.load_csr_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;

BEGIN

batch_start_time := CURRENT_TIMESTAMP; -- Start time for the batch operation
RAISE NOTICE '================================';
RAISE NOTICE 'Loading Bronze Layer Data...';
RAISE NOTICE '================================';


RAISE NOTICE '------------------------------------------------';
RAISE NOTICE 'Loading CSR Company Information Data...';
RAISE NOTICE '------------------------------------------------';


start_time := CURRENT_TIMESTAMP; -- Start time for the operation
RAISE NOTICE '>> Truncating table: bronze.csr_company...';
-- Truncate the existing tables in the bronze layer
TRUNCATE TABLE bronze.csr_company; -- Clear the table before inserting new data
RAISE NOTICE '>> Bulk inserting data into bronze.csr_company...'; 
-- Bulk insert data from CSV files into the bronze layer tables
COPY bronze.csr_company
FROM 'C:\Users\colec\OneDrive\Documents\GitHub\PetroEnergy_DataWarehousing\datasets\source_csr\csr_company.csv'
DELIMITER ',' CSV HEADER;

end_time := CURRENT_TIMESTAMP; -- End time for the operation
RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
RAISE NOTICE '-----------------';


RAISE NOTICE '------------------------------------------------';
RAISE NOTICE 'Loading CSR Accomplishments Data...';
RAISE NOTICE '------------------------------------------------';


start_time := CURRENT_TIMESTAMP; -- Start time for the operation
RAISE NOTICE '>> Truncating table: bronze.csr_accomplishments...';
-- Truncate the existing tables in the bronze layer
TRUNCATE TABLE bronze.csr_accomplishments; -- Clear the table before inserting new data
RAISE NOTICE '>> Bulk inserting data into bronze.csr_accomplishments...'; 
-- Bulk insert data from CSV files into the bronze layer tables
COPY bronze.csr_accomplishments
FROM 'C:\Users\colec\OneDrive\Documents\GitHub\PetroEnergy_DataWarehousing\datasets\source_csr\csr_accomplishments.csv'
DELIMITER ',' CSV HEADER;

end_time := CURRENT_TIMESTAMP; -- End time for the operation
RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
RAISE NOTICE '-----------------';

batch_end_time := CURRENT_TIMESTAMP; -- End time for the batch operation
RAISE NOTICE '================================';
RAISE NOTICE 'Loading CSR Bronze Layer is Completed';
RAISE NOTICE '     - Total Load Duraton: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
RAISE NOTICE '================================';

EXCEPTION
WHEN OTHERS THEN
RAISE NOTICE '================================';
RAISE NOTICE 'Error occurred while loading data: %', SQLERRM;
RAISE NOTICE '================================';
END;
$$;