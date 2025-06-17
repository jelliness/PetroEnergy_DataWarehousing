CREATE OR REPLACE PROCEDURE bronze.load_hr_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    today_str TEXT;
    latest_sequence INT;
BEGIN
    batch_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading HR Bronze Layer Data...';
    RAISE NOTICE '================================';

    -- =========================
    -- hr_demographics
    -- =========================
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Demographics Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.hr_demographics;
    COPY bronze.hr_demographics
    FROM 'C:/Github/PetroEnergy_DataWarehousing/datasets/source_hr/hr_demographics.csv'
    DELIMITER ',' CSV HEADER;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- =========================
    -- hr_parental_leave
    -- =========================
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Parental Leave Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.hr_parental_leave;
    TRUNCATE TABLE bronze.hr_parental_leave_staging;
    COPY bronze.hr_parental_leave_staging (employee_id, type_of_leave, date, days)
    FROM 'C:/Github/PetroEnergy_DataWarehousing/datasets/source_hr/hr_parental_leave.csv'
    DELIMITER ',' CSV HEADER;

    today_str := TO_CHAR(CURRENT_DATE, 'YYYYMMDD');
    SELECT COALESCE(MAX(CAST(SUBSTRING(parental_leave_id, 11, 4) AS INT)), 0)
    INTO latest_sequence
    FROM silver.hr_parental_leave
    WHERE SUBSTRING(parental_leave_id, 3, 8) = today_str;

    INSERT INTO bronze.hr_parental_leave (parental_leave_id, employee_id, type_of_leave, date, days)
    SELECT
        'PL' || today_str || LPAD((latest_sequence + ROW_NUMBER() OVER (ORDER BY employee_id, date))::TEXT, 4, '0'),
        s.employee_id,
        s.type_of_leave,
        s.date,
        s.days
    FROM bronze.hr_parental_leave_staging s;
    TRUNCATE TABLE bronze.hr_parental_leave_staging;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- =========================
    -- hr_tenure
    -- =========================
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Tenure Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.hr_tenure;
    COPY bronze.hr_tenure
    FROM 'C:/Github/PetroEnergy_DataWarehousing/datasets/source_hr/hr_tenure.csv'
    DELIMITER ',' CSV HEADER;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- =========================
    -- hr_training
    -- =========================
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Training Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.hr_training;
    TRUNCATE TABLE bronze.hr_training_staging;
    COPY bronze.hr_training_staging (company_id, training_title, date, training_hours, number_of_participants)
    FROM 'C:/Github/PetroEnergy_DataWarehousing/datasets/source_hr/hr_training.csv'
    DELIMITER ',' CSV HEADER;

    today_str := TO_CHAR(CURRENT_DATE, 'YYYYMMDD');
    SELECT COALESCE(MAX(CAST(SUBSTRING(training_id, 11, 4) AS INT)), 0)
    INTO latest_sequence
    FROM silver.hr_training
    WHERE SUBSTRING(training_id, 3, 8) = today_str;

    INSERT INTO bronze.hr_training (training_id, company_id, training_title, date, training_hours, number_of_participants)
    SELECT
        'TR' || today_str || LPAD((latest_sequence + ROW_NUMBER() OVER (ORDER BY company_id, training_title, date))::TEXT, 4, '0'),
        s.company_id,
        s.training_title,
        s.date,
        s.training_hours,
        s.number_of_participants
    FROM bronze.hr_training_staging s;
    TRUNCATE TABLE bronze.hr_training_staging;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- =========================
    -- hr_safety_workdata
    -- =========================
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Safety Workdata...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.hr_safety_workdata;
    TRUNCATE TABLE bronze.hr_safety_workdata_staging;
    COPY bronze.hr_safety_workdata_staging (company_id, contractor, date, manpower, manhours)
    FROM 'C:/Github/PetroEnergy_DataWarehousing/datasets/source_hr/hr_safety_workdata.csv'
    DELIMITER ',' CSV HEADER;

    today_str := TO_CHAR(CURRENT_DATE, 'YYYYMMDD');
    SELECT COALESCE(MAX(CAST(SUBSTRING(safety_workdata_id, 11, 4) AS INT)), 0)
    INTO latest_sequence
    FROM silver.hr_safety_workdata
    WHERE SUBSTRING(safety_workdata_id, 3, 8) = today_str;

    INSERT INTO bronze.hr_safety_workdata (safety_workdata_id, company_id, contractor, date, manpower, manhours)
    SELECT
        'SWD' || today_str || LPAD((latest_sequence + ROW_NUMBER() OVER (ORDER BY company_id, contractor, date))::TEXT, 4, '0'),
        s.company_id,
        s.contractor,
        s.date,
        s.manpower,
        s.manhours
    FROM bronze.hr_safety_workdata_staging s;
    TRUNCATE TABLE bronze.hr_safety_workdata_staging;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- =========================
    -- hr_occupational_safety_health
    -- =========================
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading HR Occupational Safety Health Data...';
    RAISE NOTICE '------------------------------------------------';

    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE bronze.hr_occupational_safety_health;
    TRUNCATE TABLE bronze.hr_occupational_safety_health_staging;
    COPY bronze.hr_occupational_safety_health_staging (company_id, workforce_type, lost_time, date, incident_type, incident_title, incident_count)
    FROM 'C:/Github/PetroEnergy_DataWarehousing/datasets/source_hr/hr_occupational_safety_health.csv'
    DELIMITER ',' CSV HEADER;

    today_str := TO_CHAR(CURRENT_DATE, 'YYYYMMDD');
    SELECT COALESCE(MAX(CAST(SUBSTRING(osh_id, 11, 4) AS INT)), 0)
    INTO latest_sequence
    FROM silver.hr_occupational_safety_health
    WHERE SUBSTRING(osh_id, 3, 8) = today_str;

    INSERT INTO bronze.hr_occupational_safety_health (osh_id, company_id, workforce_type, lost_time, date, incident_type, incident_title, incident_count)
    SELECT
        'OSH' || today_str || LPAD((latest_sequence + ROW_NUMBER() OVER (ORDER BY company_id, date, incident_type))::TEXT, 4, '0'),
        s.company_id,
        s.workforce_type,
        s.lost_time,
        s.date,
        s.incident_type,
        s.incident_title,
        s.incident_count
    FROM bronze.hr_occupational_safety_health_staging s;
    TRUNCATE TABLE bronze.hr_occupational_safety_health_staging;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- =========================
    -- Done
    -- =========================
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