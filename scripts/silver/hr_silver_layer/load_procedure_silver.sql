CREATE OR REPLACE PROCEDURE silver.load_hr_silver()
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
    RAISE NOTICE 'Loading HR Silver Layer Data...';
    RAISE NOTICE '================================';

    -- hr_demographics
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Demographics Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Inserting Data into silver.hr_demographics...';

	-- INSERT INTO silver.hr_demographics SELECT * FROM bronze.hr_demographics;

	INSERT INTO silver.hr_demographics (
	    employee_id,
    	gender,
    	birthdate,
    	age,
    	position_id,
    	position_name,
    	p_np,
    	company_id,
    	date_created,
    	date_updated
	)
	SELECT
    	b.employee_id,
    	b.gender,
    	b.birthdate,
    	DATE_PART('year', AGE(CURRENT_DATE, b.birthdate))::INT, -- derived age
    	b.position_id,
    	b.position_name,
    	b.p_np,
    	b.company_id,
    	CURRENT_TIMESTAMP, -- date_created
    	CURRENT_TIMESTAMP  -- date_updated
	FROM bronze.hr_demographics b
	ON CONFLICT (employee_id)
	DO UPDATE SET
    	gender = EXCLUDED.gender,
    	birthdate = EXCLUDED.birthdate,
    	age = EXCLUDED.age,
    	position_id = EXCLUDED.position_id,
    	position_name = EXCLUDED.position_name,
    	p_np = EXCLUDED.p_np,
    	company_id = EXCLUDED.company_id,
    	date_updated = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

-----------------------------------------------------------------------------------------------------

    -- hr_parental_leave
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Parental Leave Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Inserting Data into silver.hr_parental_leave...';

	-- REMOVES DUPLICATE
	DELETE FROM silver.hr_parental_leave
	WHERE (employee_id, date) IN (
    	SELECT employee_id, date FROM bronze.hr_parental_leave
	);


	INSERT INTO silver.hr_parental_leave (
    	employee_id, 
		type_of_leave, 
		date, days, 
		end_date, 
		months_availed, 
		date_created, 
		date_updated
	)
	SELECT
    	b.employee_id,
    	b.type_of_leave,
    	b.date,
    	b.days,
    	b.date + (b.days || ' days')::INTERVAL,
    	FLOOR(b.days / 30),
    	CURRENT_TIMESTAMP,
    	CURRENT_TIMESTAMP
	FROM bronze.hr_parental_leave b;


    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';
	
-----------------------------------------------------------------------------------------------------

    -- hr_tenure
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Tenure Data...';
    RAISE NOTICE '------------------------------------------------';

	DELETE FROM silver.hr_tenure
	WHERE employee_id IN (SELECT employee_id FROM bronze.hr_tenure);

	INSERT INTO silver.hr_tenure (
    employee_id, start_date, end_date, is_active, tenure_length, date_created, date_updated
	)
	SELECT
    	b.employee_id,
    	b.start_date,
    	b.end_date,
    	b.end_date IS NULL,
    	ROUND(EXTRACT(DAY FROM COALESCE(b.end_date, CURRENT_DATE) - b.start_date) / 365.0, 2), -- TO BE UPDATED
    	CURRENT_TIMESTAMP,
    	CURRENT_TIMESTAMP
	FROM bronze.hr_tenure b;
	
    start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Inserting Data into silver.hr_tenure...';

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

-----------------------------------------------------------------------------------------------------

    -- hr_training
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Training Data...';
    RAISE NOTICE '------------------------------------------------';

    RAISE NOTICE '>> Inserting data into silver.hr_training...';

	DELETE FROM silver.hr_training
	WHERE (employee_id, month_start, year_start) IN (
    	SELECT employee_id, month_start, year_start FROM bronze.hr_training
	);

	INSERT INTO silver.hr_training (
    	employee_id, 
		hours, 
		month_start, 
		year_start, 
		categories_per_level, 
		date_created, 
		date_updated
	)
	SELECT
    	b.employee_id,
    	b.hours,
    	b.month_start,
    	b.year_start,
    	b.categories_per_level,
    	CURRENT_TIMESTAMP,
    	CURRENT_TIMESTAMP
	FROM bronze.hr_training b;


    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

-----------------------------------------------------------------------------------------------------

    -- hr_safety
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Safety Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Inserting data into silver.hr_safety...';

	DELETE FROM silver.hr_safety
	WHERE (employee_id, date) IN (
    	SELECT employee_id, date FROM bronze.hr_safety
	);

	INSERT INTO silver.hr_safety (
    	employee_id, 
		company_id, 
		date, 
		type_of_accident, 
		safety_man_hours, 
		date_created, 
		date_updated
	)
	SELECT
    	b.employee_id,
    	b.company_id,
    	b.date,
    	b.type_of_accident,
    	b.safety_man_hours,
    	CURRENT_TIMESTAMP,
    	CURRENT_TIMESTAMP
	FROM bronze.hr_safety b;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

-----------------------------------------------------------------------------------------------------

    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading HR silver Layer is Completed';
    RAISE NOTICE '     - Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$$;