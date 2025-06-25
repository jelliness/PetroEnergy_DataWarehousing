/*
===============================================================================
Stored Procedure: Insert Trimmed Data from Bronze to Silver
===============================================================================
Script Purpose:
    This procedure transfers data from bronze tables to silver tables,
    trimming string values and handling data transformation.
    
    The procedure follows the standard ETL pattern with detailed logging
    of execution times and error handling.
===============================================================================
*/

-- Create or replace the master procedure
CREATE OR REPLACE PROCEDURE silver.load_csv_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';
    
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CSV Tables';
    RAISE NOTICE '------------------------------------------------';
    
    BEGIN


	    -- Loading silver.csv_energy_records
        -- Upserting into silver.csv_energy_records
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upserting Data Into: silver.csv_energy_records';

		INSERT INTO silver.csv_energy_records (
		    energy_id,
		    power_plant_id,
		    date_generated,
		    energy_generated_kwh,
		    co2_avoidance_kg,
			create_at,
		    updated_at
		)
		SELECT 
		energy_id,
		er.power_plant_id,
		CASE
			WHEN datetime::text ~ '^\d{1,2}/\d{1,2}/\d{4} \d{2}:\d{2}$' THEN 
				TO_TIMESTAMP(datetime::text, 'FMMM/FMDD/YYYY HH24:MI')
			ELSE 
				datetime::timestamp
		END AS date_generated,
		ROUND(
			CASE
				WHEN unit_of_measurement ILIKE 'MWh' THEN COALESCE(energy_generated, 0) * 1000
				WHEN unit_of_measurement ILIKE 'GWh' THEN COALESCE(energy_generated, 0) * 1000000
				WHEN unit_of_measurement ILIKE 'kWh' THEN COALESCE(energy_generated, 0)
				ELSE 0
			END,
			4
		) AS energy_generated,

		ROUND(
			(
				CASE
					WHEN unit_of_measurement ILIKE 'MWh' THEN COALESCE(energy_generated, 0) * 1000
					WHEN unit_of_measurement ILIKE 'GWh' THEN COALESCE(energy_generated, 0) * 1000000
					WHEN unit_of_measurement ILIKE 'kWh' THEN COALESCE(energy_generated, 0)
					ELSE 0
				END
			) * COALESCE(CAST(ef.kg_co2_per_kwh AS NUMERIC), 0) 
			- 
				CASE 
					WHEN ef.generation_source ILIKE 'geothermal' THEN 
						ROUND(
							(
							CASE
								WHEN unit_of_measurement ILIKE 'MWh' THEN COALESCE(energy_generated, 0) * 1000
								WHEN unit_of_measurement ILIKE 'GWh' THEN COALESCE(energy_generated, 0) * 1000000
								WHEN unit_of_measurement ILIKE 'kWh' THEN COALESCE(energy_generated, 0)
								ELSE 0
							END
							) * COALESCE(ef.co2_emitted_kg, 0),
						4)
					ELSE 0
				END,
			4
		) AS co2_avoidance,
		NOW(),
		NOW()
		FROM bronze.csv_energy_records er
		LEFT JOIN ref.ref_power_plants pp ON pp.power_plant_id = er.power_plant_id
		LEFT JOIN ref.ref_emission_factors ef ON pp.ef_id = ef.ef_id
		ORDER BY energy_id,date_generated

		ON CONFLICT (energy_id) DO UPDATE
		SET 
			power_plant_id = EXCLUDED.power_plant_id,
			date_generated = EXCLUDED.date_generated,
			energy_generated_kwh = EXCLUDED.energy_generated_kwh,
			co2_avoidance_kg = EXCLUDED.co2_avoidance_kg,
			updated_at = NOW();
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upsert Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';
		RAISE NOTICE '>> Total Records Upserted: %',
			(SELECT COUNT(*) FROM silver.csv_energy_records);

		

        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '==========================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE 'Error Code: %', SQLSTATE;
            RAISE NOTICE '==========================================';
    END;
END;
$$;


call silver.load_csv_silver()

