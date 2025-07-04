-- This script is used to create a stored procedure that performs the following tasks:

-- UTILIZATION: 'EXEC bronze.load_bronze;'

-- This script is used to create a stored procedure that performs the following tasks:
-- 1. Truncates the existing tables in the bronze layer.
-- 2. Bulk inserts data from CSV files into the bronze layer tables.
-- 3. The script includes the necessary configurations for the bulk insert operation.
-- 4. The script also includes error handling to catch any errors that occur during the execution of the procedure.
-- 5. The script prints the start and end time of each operation, as well as the total duration of the batch operation.

CREATE OR REPLACE PROCEDURE bronze.load_envi_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
	local_file_path TEXT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
	local_file_path :=  'C:\Users\acct\Documents\GitHub Repos\PetroEnergy_DataWarehousing\datasets\source_envi';		-- Change this according to your folder's file path 
	batch_start_time := CURRENT_TIMESTAMP; -- Start time for the batch operation
	RAISE NOTICE '================================';
	RAISE NOTICE 'Loading Environment Data into Bronze Layer...';
	RAISE NOTICE '================================';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Company Property Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	
	-- Create temporary table for company property
    CREATE TEMP TABLE temp_company_property (LIKE bronze.envi_company_property);
	
	-- Bulk insert data from CSV file into the temp table
	EXECUTE format(
	    'COPY temp_company_property FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_company_property.csv'
	);

	-- Upsert from temporary table to bronze
    INSERT INTO bronze.envi_company_property
    SELECT * FROM temp_company_property
    ON CONFLICT (cp_id)
    DO UPDATE SET
		company_id = EXCLUDED.company_id,
		cp_name = EXCLUDED.cp_name,
		cp_type = EXCLUDED.cp_type;

	DROP TABLE temp_company_property;	
	
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Water Abstraction Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation

	-- Create temporary table for naturlal sources
	CREATE TEMP TABLE temp_water_abstraction (LIKE bronze.envi_water_abstraction);
	
	-- Load CSV into temporary table
	EXECUTE format(
	    'COPY temp_water_abstraction FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_water_abstraction.csv'
	);

	-- Upsert from temporary table to bronze
    INSERT INTO bronze.envi_water_abstraction
    SELECT * FROM temp_water_abstraction
    ON CONFLICT (wa_id)
    DO UPDATE SET
		company_id = EXCLUDED.company_id,
		year = EXCLUDED.year,
		month = EXCLUDED.month,
		quarter = EXCLUDED.quarter,
		volume = EXCLUDED.volume,
		unit_of_measurement = EXCLUDED.unit_of_measurement;

	DROP TABLE temp_water_abstraction;
	
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Water Discharge Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation

	-- Create temporary table for naturlal sources
	CREATE TEMP TABLE temp_water_discharge (LIKE bronze.envi_water_discharge);
	
	-- Load CSV into temporary table
	EXECUTE format(
	    'COPY temp_water_discharge FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_water_discharge.csv'
	);

	-- Upsert from temporary table to bronze
    INSERT INTO bronze.envi_water_discharge
    SELECT * FROM temp_water_discharge
    ON CONFLICT (wd_id)
    DO UPDATE SET
		company_id = EXCLUDED.company_id,
		year = EXCLUDED.year,
		quarter = EXCLUDED.quarter,
		volume = EXCLUDED.volume,
		unit_of_measurement = EXCLUDED.unit_of_measurement;

	DROP TABLE temp_water_discharge;
	
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Water Consumption Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation

	-- Create temporary table for naturlal sources
	CREATE TEMP TABLE temp_water_consumption (LIKE bronze.envi_water_consumption);
	
	-- Load CSV into temporary table
	EXECUTE format(
	    'COPY temp_water_consumption FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_water_consumption.csv'
	);

	-- Upsert from temporary table to bronze
    INSERT INTO bronze.envi_water_consumption
    SELECT * FROM temp_water_consumption
    ON CONFLICT (wc_id)
    DO UPDATE SET
		company_id = EXCLUDED.company_id,
		year = EXCLUDED.year,
		quarter = EXCLUDED.quarter,
		volume = EXCLUDED.volume,
		unit_of_measurement = EXCLUDED.unit_of_measurement;

	DROP TABLE temp_water_consumption;
	
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Diesel Consumption Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	
	-- Create temporary table for naturlal sources
	CREATE TEMP TABLE temp_diesel_consumption (LIKE bronze.envi_diesel_consumption);
	
	-- Load CSV into temporary table
	EXECUTE format(
	    'COPY temp_diesel_consumption FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_diesel_consumption.csv'
	);

	-- Upsert from temporary table to bronze
    INSERT INTO bronze.envi_diesel_consumption
    SELECT * FROM temp_diesel_consumption
    ON CONFLICT (dc_id)
    DO UPDATE SET
		company_id = EXCLUDED.company_id,
		cp_id = EXCLUDED.cp_id,
		unit_of_measurement = EXCLUDED.unit_of_measurement,
		consumption = EXCLUDED.consumption,
		date = EXCLUDED.date;
		
	DROP TABLE temp_diesel_consumption;	
	
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';
	

	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Electric Consumption Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	
	-- Create temporary table for naturlal sources
	CREATE TEMP TABLE temp_electric_consumption (LIKE bronze.envi_electric_consumption);
	
	-- Load CSV into temporary table
	EXECUTE format(
	    'COPY temp_electric_consumption FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_electric_consumption.csv'
	);

	-- Upsert from temporary table to bronze
    INSERT INTO bronze.envi_electric_consumption
    SELECT * FROM temp_electric_consumption
    ON CONFLICT (ec_id)
    DO UPDATE SET
		company_id = EXCLUDED.company_id,
		source = EXCLUDED.source,
		unit_of_measurement = EXCLUDED.unit_of_measurement,
		consumption = EXCLUDED.consumption,
		quarter = EXCLUDED.quarter,
		year = EXCLUDED.year;

	DROP TABLE temp_electric_consumption;
	
	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Non-Hazardous Waste Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	
	-- Create temporary table for naturlal sources
	CREATE TEMP TABLE temp_non_hazard_waste (LIKE bronze.envi_non_hazard_waste);
	
	-- Load CSV into temporary table
	EXECUTE format(
	    'COPY temp_non_hazard_waste FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_non_hazard_waste.csv'
	);

	-- Upsert from temporary table to bronze
    INSERT INTO bronze.envi_non_hazard_waste
    SELECT * FROM temp_non_hazard_waste
    ON CONFLICT (nhw_id)
    DO UPDATE SET
		company_id = EXCLUDED.company_id,
		metrics = EXCLUDED.metrics,
		unit_of_measurement = EXCLUDED.unit_of_measurement,
		waste = EXCLUDED.waste,
		month = EXCLUDED.month,
		quarter = EXCLUDED.quarter,
		year = EXCLUDED.year;

	DROP TABLE temp_non_hazard_waste;

	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


    RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Hazardous Waste Generated Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	
	-- Create temporary table for naturlal sources
	CREATE TEMP TABLE temp_hazard_waste_generated (LIKE bronze.envi_hazard_waste_generated);
	
	-- Load CSV into temporary table
	EXECUTE format(
	    'COPY temp_hazard_waste_generated FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_hazard_waste_generated.csv'
	);

	-- Upsert from temporary table to bronze
    INSERT INTO bronze.envi_hazard_waste_generated
    SELECT * FROM temp_hazard_waste_generated
    ON CONFLICT (hwg_id)
    DO UPDATE SET
		company_id = EXCLUDED.company_id,
		metrics = EXCLUDED.metrics,
		unit_of_measurement = EXCLUDED.unit_of_measurement,
		waste_generated = EXCLUDED.waste_generated,
		quarter = EXCLUDED.quarter,
		year = EXCLUDED.year;

	DROP TABLE temp_hazard_waste_generated;

	end_time := CURRENT_TIMESTAMP; -- End time for the operation
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	RAISE NOTICE '-----------------';


	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading Hazardous Waste Disposed Data...';
	RAISE NOTICE '------------------------------------------------';


	start_time := CURRENT_TIMESTAMP; -- Start time for the operation
	
	-- Create temporary table for naturlal sources
	CREATE TEMP TABLE temp_hazard_waste_disposed (LIKE bronze.envi_hazard_waste_disposed);
	
	-- Load CSV into temporary table
	EXECUTE format(
	    'COPY temp_hazard_waste_disposed FROM %L DELIMITER '','' CSV HEADER',
	    local_file_path || '\envi_hazard_waste_disposed.csv'
	);

	-- Upsert from temporary table to bronze
    INSERT INTO bronze.envi_hazard_waste_disposed
    SELECT * FROM temp_hazard_waste_disposed
    ON CONFLICT (hwd_id)
    DO UPDATE SET
		company_id = EXCLUDED.company_id,
		metrics = EXCLUDED.metrics,
		unit_of_measurement = EXCLUDED.unit_of_measurement,
		waste_disposed = EXCLUDED.waste_disposed,
		year = EXCLUDED.year;

	DROP TABLE temp_hazard_waste_disposed;

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