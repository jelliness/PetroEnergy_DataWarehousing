CREATE OR REPLACE PROCEDURE bronze.load_csv_bronze(local_file_path TEXT)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
BEGIN
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading Bronze Layer Data with UPSERT...';
    RAISE NOTICE '================================';

    -- Set session timezone to Asia/Manila
    SET TIME ZONE 'Asia/Manila';
    RAISE NOTICE 'Session time zone set to %', current_setting('TIMEZONE');

    
    -- ====================
    -- Load csv_energy_records
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading csv_energy_records with UPSERT...';

    DROP TABLE IF EXISTS csv_energy_records;
    CREATE TEMP TABLE csv_energy_records (
        power_plant_id TEXT,
        datetime TEXT,
        energy_generated NUMERIC,
        unit_of_measurement TEXT
    );

    EXECUTE format(
        'COPY csv_energy_records FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/csv_energy_records.csv'
    );

    INSERT INTO bronze.csv_energy_records (
        power_plant_id, datetime, energy_generated, unit_of_measurement
    )
    SELECT power_plant_id, datetime, energy_generated, unit_of_measurement
    FROM csv_energy_records
    ON CONFLICT (power_plant_id, datetime) DO UPDATE
    SET
        energy_generated = EXCLUDED.energy_generated,
        unit_of_measurement = EXCLUDED.unit_of_measurement,
        updated_at = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$BODY$;
