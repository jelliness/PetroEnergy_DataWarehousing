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
		TO_TIMESTAMP(REPLACE(datetime, '-', '/'), 'FMMM/FMDD/YYYY HH24:MI')::timestamp AS date_generated,
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

		-- Upserting into silver.csv_funds_allocation
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upserting Data Into: silver.csv_funds_allocation';

		INSERT INTO silver.csv_funds_allocation (
			month_generated,
			power_plant_id,
			ff_id, 
			power_generated_peso,
			funds_allocated_peso,
			create_at,
			updated_at
		)
		SELECT 
			DATE_TRUNC('month', er.date_generated) AS month_generated,
			pp.power_plant_id,
			ff.ff_id,
			ROUND(SUM(er.energy_generated_kwh * 0.01), 2) AS power_generated_peso,
			ROUND(SUM((er.energy_generated_kwh * 0.01) * ff.ff_percentage), 2) AS funds_allocated_peso,
			NOW(),
			NOW()
		FROM silver.csv_energy_records er
		LEFT JOIN ref.ref_power_plants pp ON pp.power_plant_id = er.power_plant_id
		CROSS JOIN ref.ref_fa_factors ff
		GROUP BY 
			DATE_TRUNC('month', er.date_generated),
			pp.power_plant_id,
			ff.ff_id

		ON CONFLICT (month_generated, power_plant_id, ff_id) DO UPDATE
		SET 
			power_generated_peso = EXCLUDED.power_generated_peso,
			funds_allocated_peso = EXCLUDED.funds_allocated_peso,
			updated_at = NOW();

		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upsert Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';


		

        
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

