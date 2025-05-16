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

	INSERT INTO silver.hr_demographics SELECT * FROM bronze.hr_demographics;
	

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    -- hr_parental_leave
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Parental Leave Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Inserting Data into silver.hr_parental_leave...';

	INSERT INTO silver.hr_parental_leave SELECT * FROM bronze.hr_parental_leave;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    -- hr_tenure
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Tenure Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Inserting Data into silver.hr_tenure...';

	INSERT INTO silver.hr_tenure SELECT * FROM bronze.hr_tenure;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    -- hr_training
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Training Data...';
    RAISE NOTICE '------------------------------------------------';

    RAISE NOTICE '>> Inserting data into silver.hr_training...';

	INSERT INTO silver.hr_demographics SELECT * FROM bronze.hr_demographics;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';
	
    -- hr_safety
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Safety Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Inserting data into silver.hr_safety...';

	INSERT INTO silver.hr_safety SELECT * FROM bronze.hr_safety;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

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