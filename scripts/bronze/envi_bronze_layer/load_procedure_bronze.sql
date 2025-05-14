-- This script is used to create a stored procedure that performs the following tasks:

-- UTILIZATION: 'EXEC bronze.load_bronze;'

-- This script is used to create a stored procedure that performs the following tasks:
-- 1. Truncates the existing tables in the bronze layer.
-- 2. Bulk inserts data from CSV files into the bronze layer tables.
-- 3. The script includes the necessary configurations for the bulk insert operation.
-- 4. The script also includes error handling to catch any errors that occur during the execution of the procedure.
-- 5. The script prints the start and end time of each operation, as well as the total duration of the batch operation.

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
	local_file_path TEXT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
	local_file_path :=  'C:\Users\kane5\Documents\GitHub\PetroEnergy_DataWarehousing\datasets\source_envi';		-- Change this according to your folder's file path 
	batch_start_time := CURRENT_TIMESTAMP; -- Start time for the batch operation
	RAISE NOTICE '================================';
	RAISE NOTICE 'Loading Bronze Layer Data...';
	RAISE NOTICE '================================';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Company Information Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	RAISE NOTICE '>> Truncating table: bronze.envi_company_info...';
	-- Truncate the existing tables in the bronze layer
	TRUNCATE TABLE bronze.envi_company_info; -- Clear the table before inserting new data
	RAISE NOTICE '>> Bulk inserting data into bronze.envi_company_info...'; 
	-- Bulk insert data from CSV files into the bronze layer tables
	EXECUTE format(
	    'COPY bronze.envi_company_info FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_company_info.csv'
	);
		
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Company Property Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	RAISE NOTICE '>> Truncating table: bronze.envi_company_property...';
	-- Truncate the existing tables in the bronze layer
	TRUNCATE TABLE bronze.envi_company_property; -- Clear the table before inserting new data
	RAISE NOTICE '>> Bulk inserting data into bronze.envi_company_property...'; 
	-- Bulk insert data from CSV files into the bronze layer tables
	EXECUTE format(
	    'COPY bronze.envi_company_property FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_company_property.csv'
	);
	
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Natural Sources Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	RAISE NOTICE '>> Truncating table: bronze.envi_natural_sources...';
	-- Truncate the existing tables in the bronze layer
	TRUNCATE TABLE bronze.envi_natural_sources; -- Clear the table before inserting new data
	RAISE NOTICE '>> Bulk inserting data into bronze.envi_natural_sources...'; 
	-- Bulk insert data from CSV files into the bronze layer tables
	EXECUTE format(
	    'COPY bronze.envi_natural_sources FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_natural_sources.csv'
	);
	
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Water Withdrawal Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	RAISE NOTICE '>> Truncating table: bronze.envi_water_withdrawal...';
	-- Truncate the existing tables in the bronze layer
	TRUNCATE TABLE bronze.envi_water_withdrawal; -- Clear the table before inserting new data
	RAISE NOTICE '>> Bulk inserting data into bronze.envi_water_withdrawal...'; 
	-- Bulk insert data from CSV files into the bronze layer tables
	EXECUTE format(
	    'COPY bronze.envi_water_withdrawal FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_water_withdrawal.csv'
	);
	
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Diesel Consumption Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	RAISE NOTICE '>> Truncating table: bronze.envi_diesel_consumption...';
	-- Truncate the existing tables in the bronze layer
	TRUNCATE TABLE bronze.envi_diesel_consumption; -- Clear the table before inserting new data
	RAISE NOTICE '>> Bulk inserting data into bronze.envi_diesel_consumption...'; 
	-- Bulk insert data from CSV files into the bronze layer tables
	EXECUTE format(
	    'COPY bronze.envi_diesel_consumption FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_diesel_consumption.csv'
	);
	
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';
	

	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Electric Consumption Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	RAISE NOTICE '>> Truncating table: bronze.envi_electric_consumption...';
	-- Truncate the existing tables in the bronze layer
	TRUNCATE TABLE bronze.envi_electric_consumption; -- Clear the table before inserting new data
	RAISE NOTICE '>> Bulk inserting data into bronze.envi_electric_consumption...'; 
	-- Bulk insert data from CSV files into the bronze layer tables
	EXECUTE format(
	    'COPY bronze.envi_electric_consumption FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_electric_consumption.csv'
	);
	
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Power Generation Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	RAISE NOTICE '>> Truncating table: bronze.envi_power_generation...';
	-- Truncate the existing tables in the bronze layer
	TRUNCATE TABLE bronze.envi_power_generation; -- Clear the table before inserting new data
	RAISE NOTICE '>> Bulk inserting data into bronze.envi_power_generation...'; 
	-- Bulk insert data from CSV files into the bronze layer tables
	EXECUTE format(
	    'COPY bronze.envi_power_generation FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_power_generation.csv'
	);

	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Non-Hazardous Waste Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	RAISE NOTICE '>> Truncating table: bronze.envi_non_hazard_waste...';
	-- Truncate the existing tables in the bronze layer
	TRUNCATE TABLE bronze.envi_non_hazard_waste; -- Clear the table before inserting new data
	RAISE NOTICE '>> Bulk inserting data into bronze.envi_non_hazard_waste...'; 
	-- Bulk insert data from CSV files into the bronze layer tables
	EXECUTE format(
	    'COPY bronze.envi_non_hazard_waste FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_non_hazard_waste.csv'
	);

	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


    RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Hazardous Waste Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	RAISE NOTICE '>> Truncating table: bronze.envi_hazard_waste...';
	-- Truncate the existing tables in the bronze layer
	TRUNCATE TABLE bronze.envi_hazard_waste; -- Clear the table before inserting new data
	RAISE NOTICE '>> Bulk inserting data into bronze.envi_hazard_waste...'; 
	-- Bulk insert data from CSV files into the bronze layer tables
	EXECUTE format(
	    'COPY bronze.envi_hazard_waste FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_hazard_waste.csv'
	);

	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


    RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Environment Activity Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	RAISE NOTICE '>> Truncating table: bronze.envi_activity...';
	-- Truncate the existing tables in the bronze layer
	TRUNCATE TABLE bronze.envi_activity; -- Clear the table before inserting new data
	RAISE NOTICE '>> Bulk inserting data into bronze.envi_activity...'; 
	-- Bulk insert data from CSV files into the bronze layer tables
	EXECUTE format(
	    'COPY bronze.envi_activity FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_activity.csv'
	);

	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


    RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Environment Activity Output Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	RAISE NOTICE '>> Truncating table: bronze.envi_activity_output...';
	-- Truncate the existing tables in the bronze layer
	TRUNCATE TABLE bronze.envi_activity_output; -- Clear the table before inserting new data
	RAISE NOTICE '>> Bulk inserting data into bronze.envi_activity_output...'; 
	-- Bulk insert data from CSV files into the bronze layer tables
	EXECUTE format(
	    'COPY bronze.envi_activity_output FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_activity_output.csv'
	);

	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	batch_end_time := CURRENT_TIMESTAMP; -- End time for the batch operation
	RAISE NOTICE '================================';
	RAISE NOTICE 'Loading Bronze Layer is Completed';
	RAISE NOTICE '     - Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
	RAISE NOTICE '================================';

EXCEPTION
	WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$$;