CREATE OR REPLACE PROCEDURE silver.load_hr_silver()
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
    RAISE NOTICE 'Loading HR Silver Layer Data...';
    RAISE NOTICE '================================';

    -- hr_demographics
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Demographics Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    -- RAISE NOTICE '>> Truncating table: silver.hr_demographics...';
    -- TRUNCATE TABLE silver.hr_demographics;
    RAISE NOTICE '>> Bulk inserting data into silver.hr_demographics...';

    COPY silver.hr_demographics
    FROM 'C:/Github/G/PetroEnergy_DataWarehousing/datasets/source_hr/hr_demographics.csv'  -- Temporary Path. Create a path
    DELIMITER ',' CSV HEADER;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    -- hr_parental_leave
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Parental Leave Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: silver.hr_parental_leave...';
    TRUNCATE TABLE silver.hr_parental_leave;
    RAISE NOTICE '>> Bulk inserting data into silver.hr_parental_leave...';

    COPY silver.hr_parental_leave
    FROM 'C:/Github/G/PetroEnergy_DataWarehousing/datasets/source_hr/hr_parental_leave.csv'  -- Temporary Path. Create a path
    DELIMITER ',' CSV HEADER;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    -- hr_tenure
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Tenure Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: silver.hr_tenure...';
    TRUNCATE TABLE silver.hr_tenure;
    RAISE NOTICE '>> Bulk inserting data into silver.hr_tenure...';

    COPY silver.hr_tenure
    FROM 'C:/Github/G/PetroEnergy_DataWarehousing/datasets/source_hr/hr_tenure.csv'  -- Temporary Path. Create a path
    DELIMITER ',' CSV HEADER;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';
/*
    -- hr_training
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Training Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: silver.hr_training...';
    TRUNCATE TABLE silver.hr_training;
    RAISE NOTICE '>> Bulk inserting data into silver.hr_training...';

    COPY silver.hr_training
    FROM 'C:/Github/G/PetroEnergy_DataWarehousing/datasets/source_hr/hr_training.csv'  -- Temporary Path. Create a path
    DELIMITER ',' CSV HEADER;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';
*/
    -- hr_safety
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Safety Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: silver.hr_safety...';
    TRUNCATE TABLE silver.hr_safety;
    RAISE NOTICE '>> Bulk inserting data into silver.hr_safety...';

    COPY silver.hr_safety
    FROM 'C:/Github/G/PetroEnergy_DataWarehousing/datasets/source_hr/hr_safety.csv'  -- Temporary Path. Create a path
    DELIMITER ',' CSV HEADER;

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