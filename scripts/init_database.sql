-- WARNING:
-- Running this script will drop the 'Petroenergy_Data_Warehousing' database if it exists.
-- All data in the database will be permanently deleted. Proceed with caution.

-- Step 1: Terminate existing connections and drop the database

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname ILIKE  '%Petroenergy_Data_Warehousing%' AND pid <> pg_backend_pid();


-- Step 2: Drop the database if it exists
DROP DATABASE IF EXISTS "PetroEnergy_Data_Warehousing";

-- Step 3: Create the database
CREATE DATABASE "PetroEnergy_Data_Warehousing";

-- ================================
-- After connecting to 'Petroenergy_Data_Warehousing', run this part
-- ================================

-- Set the time zone
SET TIME ZONE 'Asia/Manila';

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
