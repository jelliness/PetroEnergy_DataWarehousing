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
CREATE OR REPLACE PROCEDURE silver.load_envi_silver(
    load_company_property BOOLEAN DEFAULT TRUE,
    load_water_abstraction BOOLEAN DEFAULT TRUE,
    load_water_discharge BOOLEAN DEFAULT TRUE,
    load_water_consumption BOOLEAN DEFAULT TRUE,
    load_diesel_consumption BOOLEAN DEFAULT TRUE,
    load_electric_consumption BOOLEAN DEFAULT TRUE,
    load_non_hazard_waste BOOLEAN DEFAULT TRUE,
    load_hazard_waste_generated BOOLEAN DEFAULT TRUE,
    load_hazard_waste_disposed BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    r RECORD;
    last_counter INT := 0;
    last_company TEXT := '';
    last_year INT := 0;
    generated_wa_id TEXT;
    bronze_wa_id TEXT;
BEGIN
    batch_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';
    
    BEGIN
        -- Loading silver.envi_company_property
        IF load_company_property THEN
            start_time := CLOCK_TIMESTAMP();
            RAISE NOTICE '>> Inserting Data Into: silver.envi_company_property';
            INSERT INTO silver.envi_company_property (
                cp_id,
                company_id,
                cp_name,
                cp_type
            )
            SELECT
                TRIM(cp_id),
                TRIM(company_id),
                TRIM(cp_name),
                TRIM(cp_type)
            FROM bronze.envi_company_property
            ON CONFLICT (cp_id)
            DO UPDATE SET
                company_id = EXCLUDED.company_id,
                cp_name = EXCLUDED.cp_name,
                cp_type = EXCLUDED.cp_type;
            
            end_time := CLOCK_TIMESTAMP();
            RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
            RAISE NOTICE '>> -------------';
        END IF;
        
        -- Loading silver.envi_water_abstraction
        IF load_water_abstraction THEN
            start_time := CLOCK_TIMESTAMP();
            RAISE NOTICE '>> Inserting Aggregated Data Into: silver.envi_water_abstraction';

            -- Insert aggregated records where month is NOT NULL
			RAISE NOTICE '>> Inserting Non-Aggregated Data (month IS NOT NULL)';
	        FOR r IN
	            SELECT
	                company_id,
	                year,
	                CASE 
	                    WHEN LOWER(TRIM(month)) IN ('january', 'february', 'march') THEN 'Q1'
	                    WHEN LOWER(TRIM(month)) IN ('april', 'may', 'june') THEN 'Q2'
	                    WHEN LOWER(TRIM(month)) IN ('july', 'august', 'september') THEN 'Q3'
	                    WHEN LOWER(TRIM(month)) IN ('october', 'november', 'december') THEN 'Q4'
	                    ELSE 'Unknown'
	                END AS quarter,
	                SUM(CASE WHEN volume < 0 THEN 0 ELSE volume END) AS total_volume,
	                MAX(unit_of_measurement) AS unit_of_measurement,
	                -- Collect all bronze wa_ids for mapping
	                ARRAY_AGG(wa_id) AS bronze_wa_ids
	            FROM bronze.envi_water_abstraction
	            WHERE month IS NOT NULL
	            GROUP BY company_id, year,
	                    CASE 
	                        WHEN LOWER(TRIM(month)) IN ('january', 'february', 'march') THEN 'Q1'
	                        WHEN LOWER(TRIM(month)) IN ('april', 'may', 'june') THEN 'Q2'
	                        WHEN LOWER(TRIM(month)) IN ('july', 'august', 'september') THEN 'Q3'
	                        WHEN LOWER(TRIM(month)) IN ('october', 'november', 'december') THEN 'Q4'
	                        ELSE 'Unknown'
	                    END
	            ORDER BY company_id, year, quarter
	        LOOP
	            -- Reset counter for each new company-year
	            IF r.company_id <> last_company OR r.year <> last_year THEN
	                last_counter := 1;
	            ELSE
	                last_counter := last_counter + 1;
	            END IF;
	
	            -- Generate the silver wa_id
	            generated_wa_id := 'WA-' || r.company_id || '-' || r.year || '-' || LPAD(last_counter::TEXT, 3, '0');
	
	            INSERT INTO silver.envi_water_abstraction (
	                wa_id,
	                company_id,
	                volume,
	                unit_of_measurement,
	                quarter,
	                year
	            )
	            VALUES (
	                generated_wa_id,
	                r.company_id,
	                r.total_volume,
	                r.unit_of_measurement,
	                r.quarter,
	                r.year
	            )
	            ON CONFLICT (wa_id) DO UPDATE SET
	                company_id = EXCLUDED.company_id,
	                volume = CASE WHEN EXCLUDED.volume < 0 THEN 0 ELSE EXCLUDED.volume END,
	                unit_of_measurement = EXCLUDED.unit_of_measurement,
	                year = EXCLUDED.year,
	                quarter = EXCLUDED.quarter,
	                updated_at = CURRENT_TIMESTAMP;
	
	            -- Insert mappings for all bronze wa_ids that contributed to this aggregated record
	            FOREACH bronze_wa_id IN ARRAY r.bronze_wa_ids
	            LOOP
	                INSERT INTO silver.wa_id_mapping (
	                    wa_id_bronze,
	                    wa_id_silver
	                )
	                VALUES (
	                    bronze_wa_id,
	                    generated_wa_id
	                )
	                ON CONFLICT (wa_id_bronze, wa_id_silver) DO NOTHING;
	            END LOOP;
	
	            last_company := r.company_id;
	            last_year := r.year;
	        END LOOP;
	
	        RAISE NOTICE '>> Inserting Non-Aggregated Data (month IS NULL)';
	
	        FOR r IN
	            SELECT
	                wa_id,
	                company_id,
	                year,
	                quarter,
	                CASE WHEN volume < 0 THEN 0 ELSE volume END AS volume,
	                unit_of_measurement
	            FROM bronze.envi_water_abstraction
	            WHERE month IS NULL
	        LOOP
	            INSERT INTO silver.envi_water_abstraction (
	                wa_id,
	                company_id,
	                volume,
	                unit_of_measurement,
	                quarter,
	                year
	            )
	            VALUES (
	                r.wa_id,
	                r.company_id,
	                r.volume,
	                r.unit_of_measurement,
	                r.quarter,
	                r.year
	            )
	            ON CONFLICT (wa_id) DO UPDATE SET
	                company_id = EXCLUDED.company_id,
	                volume = CASE WHEN EXCLUDED.volume < 0 THEN 0 ELSE EXCLUDED.volume END,
	                unit_of_measurement = EXCLUDED.unit_of_measurement,
	                year = EXCLUDED.year,
	                quarter = EXCLUDED.quarter,
	                updated_at = CURRENT_TIMESTAMP;
	
	            -- For non-aggregated records, create a 1:1 mapping
	            INSERT INTO silver.wa_id_mapping (
	                wa_id_bronze,
	                wa_id_silver
	            )
	            VALUES (
	                r.wa_id,
	                r.wa_id
	            )
	            ON CONFLICT (wa_id_bronze, wa_id_silver) DO NOTHING;
	        END LOOP;
            
            end_time := CLOCK_TIMESTAMP();
            RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
            RAISE NOTICE '>> -------------';
        END IF;

		-- Loading silver.envi_water_discharge
        IF load_water_discharge THEN
			start_time := CLOCK_TIMESTAMP();
	        RAISE NOTICE '>> Inserting Data Into: silver.envi_water_discharge';
	        INSERT INTO silver.envi_water_discharge (
	            wd_id,
	            company_id,
	            volume,
	            unit_of_measurement,
	            quarter,
	            year
	        )
	        SELECT
	            TRIM(wd_id),
	            TRIM(company_id),
	            CASE
	                WHEN volume < 0 THEN 0  -- Handle negative values
	                ELSE volume
	            END AS volume,
	            TRIM(unit_of_measurement),
	            TRIM(quarter),
	            year
	        FROM bronze.envi_water_discharge
	        ON CONFLICT (wd_id)
	        DO UPDATE SET
	            company_id = EXCLUDED.company_id,
	            volume = CASE
	                WHEN EXCLUDED.volume < 0 THEN 0  -- Handle negative values
	                ELSE EXCLUDED.volume
	            END,
	            unit_of_measurement = EXCLUDED.unit_of_measurement,
	            quarter = EXCLUDED.quarter,
	            year = EXCLUDED.year;
	
	        end_time := CLOCK_TIMESTAMP();
	        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	        RAISE NOTICE '>> -------------';
		END IF;

		-- Loading silver.envi_water_consumption
		IF load_water_consumption THEN
			start_time := CLOCK_TIMESTAMP();
	        RAISE NOTICE '>> Inserting Data Into: silver.envi_water_consumption';
	        INSERT INTO silver.envi_water_consumption (
	            wc_id,
	            company_id,
	            volume,
	            unit_of_measurement,
	            quarter,
	            year
	        )
	        SELECT
	            TRIM(wc_id),
	            TRIM(company_id),
	            CASE
	                WHEN volume < 0 THEN 0  -- Handle negative values
	                ELSE volume
	            END AS volume,
	            TRIM(unit_of_measurement),
	            TRIM(quarter),
	            year
	        FROM bronze.envi_water_consumption
	        ON CONFLICT (wc_id)
	        DO UPDATE SET
	            company_id = EXCLUDED.company_id,
	            volume = CASE
	                WHEN EXCLUDED.volume < 0 THEN 0  -- Handle negative values
	                ELSE EXCLUDED.volume
	            END,
	            unit_of_measurement = EXCLUDED.unit_of_measurement,
	            quarter = EXCLUDED.quarter,
	            year = EXCLUDED.year;
	
	        end_time := CLOCK_TIMESTAMP();
	        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	        RAISE NOTICE '>> -------------';
		END IF;

		-- Loading silver.envi_diesel_consumption
		IF load_diesel_consumption THEN
			start_time := CLOCK_TIMESTAMP();
	        RAISE NOTICE '>> Inserting Data Into: silver.envi_diesel_consumption';
			INSERT INTO silver.envi_diesel_consumption (
			    dc_id,
			    company_id,
			    cp_id,
			    unit_of_measurement,
			    consumption,
			    month,
			    year,
			    quarter,
			    date
			)
			SELECT
			    TRIM(dc_id),
			    TRIM(company_id),
			    TRIM(cp_id),
			    TRIM(unit_of_measurement),
			    CASE
			        WHEN consumption < 0 THEN 0 -- Handle negative values
			        ELSE consumption
			    END AS consumption,
			    TRIM(TO_CHAR(date, 'Month')) AS month,
			    EXTRACT(YEAR FROM date)::INT AS year,
			    CASE
			        WHEN EXTRACT(MONTH FROM date) BETWEEN 1 AND 3 THEN 'Q1'
			        WHEN EXTRACT(MONTH FROM date) BETWEEN 4 AND 6 THEN 'Q2'
			        WHEN EXTRACT(MONTH FROM date) BETWEEN 7 AND 9 THEN 'Q3'
			        WHEN EXTRACT(MONTH FROM date) BETWEEN 10 AND 12 THEN 'Q4'
			    END AS quarter,
			    date
			FROM bronze.envi_diesel_consumption
			ON CONFLICT (dc_id) DO UPDATE SET
			    company_id = EXCLUDED.company_id,
			    cp_id = EXCLUDED.cp_id,
			    unit_of_measurement = EXCLUDED.unit_of_measurement,
			    consumption = CASE
			        WHEN EXCLUDED.consumption < 0 THEN 0 -- Handle negative values
			        ELSE EXCLUDED.consumption
			    END,
			    month = EXCLUDED.month,
			    year = EXCLUDED.year,
			    quarter = CASE
			        WHEN EXTRACT(MONTH FROM EXCLUDED.date) BETWEEN 1 AND 3 THEN 'Q1'
			        WHEN EXTRACT(MONTH FROM EXCLUDED.date) BETWEEN 4 AND 6 THEN 'Q2'
			        WHEN EXTRACT(MONTH FROM EXCLUDED.date) BETWEEN 7 AND 9 THEN 'Q3'
			        WHEN EXTRACT(MONTH FROM EXCLUDED.date) BETWEEN 10 AND 12 THEN 'Q4'
				END,
			    date = EXCLUDED.date,
			    updated_at = CURRENT_TIMESTAMP;
	
	        end_time := CLOCK_TIMESTAMP();
	        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	        RAISE NOTICE '>> -------------';
		END IF;

		-- Loading silver.envi_electric_consumption
		IF load_electric_consumption THEN
			start_time := CLOCK_TIMESTAMP();
	        RAISE NOTICE '>> Inserting Data Into: silver.envi_electric_consumption';
	        -- Loading silver.envi_electric_consumption
	        INSERT INTO silver.envi_electric_consumption (
			    ec_id,
			    company_id,
				source,
			    unit_of_measurement,
			    consumption,
			    quarter,
			    year
			)
			SELECT
			    TRIM(ec_id),
			    TRIM(company_id),
				TRIM(source),
			    TRIM(unit_of_measurement),
			    CASE
			        WHEN consumption < 0 THEN 0  -- Handle negative values
			        ELSE consumption
			    END AS consumption,
			    TRIM(quarter),
			    year
			FROM bronze.envi_electric_consumption
			ON CONFLICT (ec_id)
			DO UPDATE SET
			    company_id = EXCLUDED.company_id,
				source = EXCLUDED.source,
			    unit_of_measurement = EXCLUDED.unit_of_measurement,
			    consumption = CASE
			        WHEN EXCLUDED.consumption < 0 THEN 0  -- Handle negative values
			        ELSE EXCLUDED.consumption
			    END,
			    quarter = EXCLUDED.quarter,
			    year = EXCLUDED.year;
	
	        end_time := CLOCK_TIMESTAMP();
	        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	        RAISE NOTICE '>> -------------';
		END IF;

		-- Loading silver.envi_non_hazard_waste
		IF load_non_hazard_waste THEN
			start_time := CLOCK_TIMESTAMP();
	        RAISE NOTICE '>> Inserting Data Into: silver.envi_non_hazard_waste';
	        INSERT INTO silver.envi_non_hazard_waste (
			    nhw_id,
			    company_id,
			    metrics,
			    unit_of_measurement,
			    waste,
			    year,
			    quarter
			)
			SELECT
			    TRIM(nhw_id),
			    TRIM(company_id),
			    TRIM(metrics),
			    TRIM(unit_of_measurement),
			    CASE
			        WHEN waste < 0 THEN 0  -- Handle negative values
			        ELSE waste
			    END AS waste,
			    year,
			    CASE 
			        WHEN LOWER(TRIM(month)) IN ('january', 'february', 'march') THEN 'Q1'
			        WHEN LOWER(TRIM(month)) IN ('april', 'may', 'june') THEN 'Q2'
			        WHEN LOWER(TRIM(month)) IN ('july', 'august', 'september') THEN 'Q3'
			        WHEN LOWER(TRIM(month)) IN ('october', 'november', 'december') THEN 'Q4'
			        ELSE TRIM(quarter)
			    END AS quarter
			FROM bronze.envi_non_hazard_waste
			ON CONFLICT (nhw_id)
			DO UPDATE SET
			    company_id = EXCLUDED.company_id,
			    metrics = EXCLUDED.metrics,
			    unit_of_measurement = EXCLUDED.unit_of_measurement,
			    waste = CASE
			        WHEN EXCLUDED.waste < 0 THEN 0
			        ELSE EXCLUDED.waste
			    END,
			    year = EXCLUDED.year,
			    quarter = CASE 
			        WHEN LOWER(TRIM(EXCLUDED.quarter)) IN ('january', 'february', 'march') THEN 'Q1'
			        WHEN LOWER(TRIM(EXCLUDED.quarter)) IN ('april', 'may', 'june') THEN 'Q2'
			        WHEN LOWER(TRIM(EXCLUDED.quarter)) IN ('july', 'august', 'september') THEN 'Q3'
			        WHEN LOWER(TRIM(EXCLUDED.quarter)) IN ('october', 'november', 'december') THEN 'Q4'
			        ELSE TRIM(EXCLUDED.quarter)
			    END;
	
	        end_time := CLOCK_TIMESTAMP();
	        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	        RAISE NOTICE '>> -------------';
		END IF;

		-- Loading silver.envi_hazard_waste_generated
		IF load_hazard_waste_generated THEN
			start_time := CLOCK_TIMESTAMP();
			RAISE NOTICE '>> Inserting Data Into: silver.envi_hazard_waste_generated';
			
			INSERT INTO silver.envi_hazard_waste_generated (
			    hwg_id,
			    company_id,
			    metrics,
			    unit_of_measurement,
			    waste_generated,
			    quarter,
			    year
			)
			SELECT
			    TRIM(hwg_id),
			    TRIM(company_id),
			    TRIM(metrics),
			    CASE
			        WHEN LOWER(TRIM(unit_of_measurement)) = 'ton' THEN
			            CASE
			                WHEN TRIM(metrics) IN ('Used Oil', 'Paint/Solvent Based') THEN 'Liter'
			                ELSE 'Kilogram'
			            END
			        ELSE TRIM(unit_of_measurement)
			    END AS unit_of_measurement,
			    CASE
				    WHEN waste_generated < 0 THEN 0
				    WHEN LOWER(TRIM(unit_of_measurement)) = 'ton' THEN
				        CASE
				            WHEN TRIM(metrics) IN ('Used Oil', 'Paint/Solvent Based') THEN waste_generated * 1000 / 0.9
				            ELSE waste_generated * 1000
				        END
				    ELSE waste_generated
				END AS waste_generated,
			    TRIM(quarter),
			    year
			FROM bronze.envi_hazard_waste_generated
			ON CONFLICT (hwg_id)
			DO UPDATE SET
			    company_id = EXCLUDED.company_id,
			    metrics = EXCLUDED.metrics,
			    unit_of_measurement = EXCLUDED.unit_of_measurement,
			    waste_generated = CASE
			        WHEN EXCLUDED.waste_generated < 0 THEN 0
			        ELSE EXCLUDED.waste_generated
			    END,
			    quarter = EXCLUDED.quarter,
			    year = EXCLUDED.year;
			
			end_time := CLOCK_TIMESTAMP();
			RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
			RAISE NOTICE '>> -------------';
		END IF;

		-- Loading silver.envi_hazard_waste_disposed
		IF load_hazard_waste_disposed THEN
			start_time := CLOCK_TIMESTAMP();
			RAISE NOTICE '>> Inserting Data Into: silver.envi_hazard_waste_disposed';
			
			INSERT INTO silver.envi_hazard_waste_disposed (
			    hwd_id,
			    company_id,
			    metrics,
			    unit_of_measurement,
			    waste_disposed,
			    year
			)
			SELECT
			    TRIM(hwd_id),
			    TRIM(company_id),
			    TRIM(metrics),
			    CASE
			        WHEN LOWER(TRIM(unit_of_measurement)) = 'ton' THEN
			            CASE
			                WHEN TRIM(metrics) IN ('Used Oil', 'Paint/Solvent Based') THEN 'Liter'
			                ELSE 'Kilogram'
			            END
			        ELSE TRIM(unit_of_measurement)
			    END AS unit_of_measurement,
				CASE
				    WHEN waste_disposed < 0 THEN 0
				    WHEN LOWER(TRIM(unit_of_measurement)) = 'ton' THEN
				        CASE
				            WHEN TRIM(metrics) IN ('Used Oil', 'Paint/Solvent Based') THEN waste_disposed * 1000 / 0.9
				            ELSE waste_disposed * 1000
				        END
				    ELSE waste_disposed
				END AS waste_disposed,
			    year
			FROM bronze.envi_hazard_waste_disposed
			ON CONFLICT (hwd_id)
			DO UPDATE SET
			    company_id = EXCLUDED.company_id,
			    metrics = EXCLUDED.metrics,
			    unit_of_measurement = EXCLUDED.unit_of_measurement,
			    waste_disposed = CASE
			        WHEN EXCLUDED.waste_disposed < 0 THEN 0
			        ELSE EXCLUDED.waste_disposed
			    END,
			    year = EXCLUDED.year;
			
			end_time := CLOCK_TIMESTAMP();
			RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
			RAISE NOTICE '>> -------------';
		END IF;

		batch_end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'Loading Silver Layer is Completed';
        RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
        RAISE NOTICE '==========================================';
        
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

CALL silver.load_envi_silver(
    load_company_property := FALSE,
    load_water_abstraction := TRUE,
    load_water_discharge := FALSE,
    load_water_consumption := FALSE,
    load_diesel_consumption := FALSE,
    load_electric_consumption := FALSE,
    load_non_hazard_waste := FALSE,
    load_hazard_waste_generated := FALSE,
    load_hazard_waste_disposed := FALSE
);