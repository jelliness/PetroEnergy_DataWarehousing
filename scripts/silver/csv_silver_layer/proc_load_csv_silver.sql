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

		
		-- Upserting into silver.csv_emission_factors
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upserting Data Into: silver.csv_emission_factors';
		WITH enriched AS (
		    SELECT 
		        LOWER(generation_source::text) AS generation_source,
		        CAST(kg_co2_per_kwh AS DECIMAL(10,4)) AS kg_co2_per_kwh,
		        'EF-' || LPAD(ROW_NUMBER() OVER (ORDER BY generation_source)::text, 3, '0') AS ef_id,
		        create_at,
		        updated_at
		    FROM bronze.csv_emission_factors
		)
		INSERT INTO silver.csv_emission_factors (
		    ef_id,
		    generation_source,
		    kg_co2_per_kwh,
		    create_at,
		    updated_at
		)
		SELECT ef_id, generation_source, kg_co2_per_kwh, create_at, updated_at FROM enriched
		ON CONFLICT (ef_id) DO UPDATE
		SET 
		    generation_source = EXCLUDED.generation_source,
		    kg_co2_per_kwh = EXCLUDED.kg_co2_per_kwh,
		    updated_at = EXCLUDED.updated_at;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upsert Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';


		-- Upserting into silver.csv_power_plants
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upserting Data Into: silver.csv_power_plants';
		INSERT INTO silver.csv_power_plants (
		    power_plant_id,
		    company_id,
		    site_name,
		    site_address,
		    city_town,
		    province,
		    country,
		    zip,
		    ef_id,
		    create_at,
		    updated_at
		)
		SELECT 
		    pp.power_plant_id,
		    pp.company_id,
		    pp.site_name,
		    pp.site_address,
		    pp.city_town,
		    pp.province,
		    pp.country,
		    pp.zip,
		    ef_id,
		    pp.create_at,
		    pp.updated_at
		FROM bronze.csv_power_plants pp
		LEFT JOIN ref.company_main com ON com.company_id = pp.company_id
		ON CONFLICT (power_plant_id) DO UPDATE
		SET 
		    company_id = EXCLUDED.company_id,
		    site_name = EXCLUDED.site_name,
		    site_address = EXCLUDED.site_address,
		    city_town = EXCLUDED.city_town,
		    province = EXCLUDED.province,
		    country = EXCLUDED.country,
		    zip = EXCLUDED.zip,
		    ef_id = EXCLUDED.ef_id,
		    updated_at = EXCLUDED.updated_at;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upsert Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';


	    -- Loading silver.csv_energy_records
        -- Upserting into silver.csv_energy_records
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upserting Data Into: silver.csv_energy_records';
		-- WITH standardized_data AS(
		-- 	SELECT
		-- 		power_plant_id,
		-- 		to_timestamp(datetime, 'DD/MM/YYYY HH24:MI') AS datetime_column,
		-- 		energy_generated,
		-- 		CASE 
		-- 			WHEN unit_of_measurement = 'mwh' THEN REPLACE(energy_generated, ',', '')::NUMERIC * 1000
		-- 			ELSE REPLACE(energy_generated, ',', '')::NUMERIC
		-- 		END AS energy_generated_converted,
		-- 		unit_of_measurement,
		-- 		'kwh' AS standardized_unit
				
		-- 	FROM bronze.csv_energy_records
		-- ),
		-- ranked_data AS (
		--     SELECT
		--         * ,
		--         ROW_NUMBER() OVER (
		--         PARTITION BY datetime_column::date, power_plant_id
		--         ORDER BY datetime_column ASC
		--     ) AS rn_asc,
		--     ROW_NUMBER() OVER (
		--         PARTITION BY datetime_column::date,power_plant_id
		--         ORDER BY datetime_column DESC
		--     ) AS rn_desc
		--     FROM standardized_data
		-- ),
		-- aggregated_data AS (
		--     SELECT
		--         power_plant_id,
		--         MAX(CASE WHEN rn_asc = 1 THEN energy_generated_converted END) AS first_energy,
		--         MAX(CASE WHEN rn_desc = 1 THEN energy_generated_converted END) AS last_energy,
		--         SUM(energy_generated_converted) AS total_energy,
		--         MIN(datetime_column) AS earliest_datetime
		--     FROM ranked_data
		-- 	GROUP BY power_plant_id,datetime_column::date
		-- ),
		-- energy_id_generation AS (
		--     SELECT
		--         * ,
		--         'EN-' || TO_CHAR(earliest_datetime, 'YYYYMMDD') || '-' || LPAD(ROW_NUMBER() OVER (ORDER BY earliest_datetime)::TEXT, 3, '0') AS energy_id
		--     FROM aggregated_data
		-- ),
		-- energy_calculation AS(
		-- 	SELECT 
		-- 		eg.energy_id,
		-- 		eg.power_plant_id,
		-- 		eg.earliest_datetime as datetime,
		-- 		ROUND(
		-- 			CASE 
		-- 				WHEN cp.resources='solar' THEN eg.last_energy - eg.first_energy
		-- 				WHEN cp.resources='wind' THEN eg.total_energy
		-- 				ELSE NULL
		-- 			END, 4
		-- 		) AS energy_generated
		-- 	FROM energy_id_generation eg
		-- 	LEFT JOIN bronze.csv_power_plants pp on pp.power_plant_id = eg.power_plant_id 
		-- 	LEFT JOIN bronze.csv_company cp on cp.company_id = pp.company_id
		-- ),
		-- pp_data AS (
		--     SELECT 
		--         pp.power_plant_id,
		--         pp.company_id,
		--         pp.site_name,
		--         pp.site_address,
		--         pp.city_town,
		--         pp.province,
		--         pp.country,
		--         pp.zip,
		--         LOWER(com.resources) AS resource,
		--         CASE 
		--             WHEN LOWER(com.resources) = 'solar' THEN 'EF-001'
		--             WHEN LOWER(com.resources) = 'wind' THEN 'EF-002'
		--             ELSE NULL
		--         END AS constants
		--     FROM bronze.csv_power_plants pp
		--     LEFT JOIN bronze.csv_company com ON com.company_id = pp.company_id
		-- ),
		
		-- emission_factors AS (
		--     SELECT 
		--         'EF-' || LPAD(ROW_NUMBER() OVER (ORDER BY LOWER(generation_source))::TEXT, 3, '0') AS ef_id,
		--         LOWER(generation_source) AS generation_source,
		--         kg_co2_per_kwh
		--     FROM bronze.csv_emission_factors
		-- )
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
			CAST(
				'EN-' || 
				TO_CHAR(TO_TIMESTAMP(REPLACE(datetime, '-', '/'), 'FMMM/FMDD/YYYY HH24:MI')::timestamp, 'YYYYMMDD') || 
				'-' || 
				LPAD(
				ROW_NUMBER() OVER (
					PARTITION BY TO_CHAR(TO_TIMESTAMP(REPLACE(datetime, '-', '/'), 'FMMM/FMDD/YYYY HH24:MI')::timestamp, 'YYYYMMDD') 
					ORDER BY TO_TIMESTAMP(REPLACE(datetime, '-', '/'), 'FMMM/FMDD/YYYY HH24:MI')::timestamp
				)::TEXT, 
				3, '0')
			AS VARCHAR(20)
			) AS energy_id,
			
			er.power_plant_id,
			
			TO_TIMESTAMP(REPLACE(datetime, '-', '/'), 'FMMM/FMDD/YYYY HH24:MI')::timestamp AS date_generated,
			
			ROUND(
				CASE
				WHEN unit_of_measurement ILIKE 'MWh' THEN energy_generated * 1000
				WHEN unit_of_measurement ILIKE 'GWh' THEN energy_generated * 1000000
				WHEN unit_of_measurement ILIKE 'kWh' THEN energy_generated 
				ELSE 0
				END,
				4
			) AS energy_generated,
			
			ROUND(
				(
				CASE
					WHEN unit_of_measurement ILIKE 'MWh' THEN energy_generated * 1000
					WHEN unit_of_measurement ILIKE 'GWh' THEN energy_generated * 1000000
					WHEN unit_of_measurement ILIKE 'kWh' THEN energy_generated 
					ELSE 0
				END
				) * COALESCE(CAST(ef.kg_co2_per_kwh AS NUMERIC), 0),
				4
			) AS co2_avoidance,
			NOW(),
			NOW()

			FROM bronze.csv_energy_records er
			LEFT JOIN bronze.csv_power_plants pp ON pp.power_plant_id = er.power_plant_id
			LEFT JOIN bronze.csv_emission_factors ef ON pp.ef_id = ef.ef_id;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upsert Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';

		-- Upserting into silver.csv_fa_factors
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upserting Data Into: silver.csv_fa_factors';

		INSERT INTO silver.csv_fa_factors (
			ff_id,
			ff_name,
			ff_percentage,
			create_at,
			updated_at
		)
		SELECT 
			(ff_id)::VARCHAR(20),
			ff_name,
			(ff_percentage)::DECIMAL(5,4),
			CURRENT_TIMESTAMP,
			CURRENT_TIMESTAMP
		FROM bronze.csv_fa_factors
		ON CONFLICT (ff_id) DO UPDATE 
		SET 
			ff_name = EXCLUDED.ff_name,
			ff_percentage = EXCLUDED.ff_percentage,
			updated_at = CURRENT_TIMESTAMP;

		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upsert Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';

		-- Upserting into silver.csv_hec_factors
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE '>> Upserting Data Into: silver.csv_hec_factors';

		INSERT INTO silver.csv_hec_factors (
			hec_id,
			hec_value,
			hec_year,
			source_name,
			source_link,
			create_at,
			updated_at
		)
		SELECT 
			(hec_id)::VARCHAR(20),
			(hec_value)::DECIMAL(10,4),
			(hec_year)::INTEGER,
			source_name,
			source_link,
			CURRENT_TIMESTAMP,
			CURRENT_TIMESTAMP
		FROM bronze.csv_hec_factors
		ON CONFLICT (hec_id) DO UPDATE 
		SET 
			hec_value = EXCLUDED.hec_value,
			hec_year = EXCLUDED.hec_year,
			source_name = EXCLUDED.source_name,
			source_link = EXCLUDED.source_link,
			updated_at = CURRENT_TIMESTAMP;

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