CREATE OR REPLACE PROCEDURE bronze.load_hr_bronze()
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
    RAISE NOTICE 'Loading HR Bronze Layer Data...';
    RAISE NOTICE '================================';

    -- hr_demographics
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Demographics Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: bronze.hr_demographics...';
    TRUNCATE TABLE bronze.hr_demographics;
    RAISE NOTICE '>> Bulk inserting data into bronze.hr_demographics...';

    COPY bronze.hr_demographics
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
    RAISE NOTICE '>> Truncating table: bronze.hr_parental_leave...';
    TRUNCATE TABLE bronze.hr_parental_leave;
    RAISE NOTICE '>> Bulk inserting data into bronze.hr_parental_leave...';

    COPY bronze.hr_parental_leave
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
    RAISE NOTICE '>> Truncating table: bronze.hr_tenure...';
    TRUNCATE TABLE bronze.hr_tenure;
    RAISE NOTICE '>> Bulk inserting data into bronze.hr_tenure...';

    COPY bronze.hr_tenure
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
    RAISE NOTICE '>> Truncating table: bronze.hr_training...';
    TRUNCATE TABLE bronze.hr_training;
    RAISE NOTICE '>> Bulk inserting data into bronze.hr_training...';

    COPY bronze.hr_training
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
    RAISE NOTICE '>> Truncating table: bronze.hr_safety...';
    TRUNCATE TABLE bronze.hr_safety;
    RAISE NOTICE '>> Bulk inserting data into bronze.hr_safety...';

    COPY bronze.hr_safety
    FROM 'C:/Github/G/PetroEnergy_DataWarehousing/datasets/source_hr/hr_safety.csv'  -- Temporary Path. Create a path
    DELIMITER ',' CSV HEADER;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading HR Bronze Layer is Completed';
    RAISE NOTICE '     - Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$$;
