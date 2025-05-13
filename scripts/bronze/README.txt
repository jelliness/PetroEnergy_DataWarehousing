Steps to Proceed with the Bronze Layer (Loading Data to Database)

1. Install Posgresql 16
2. Run 'pgadmin 4' 
3. Create Database named 'Petroenergy_Data_Warehousing'
4. Create Schema named 'bronze' ('silver' and 'gold' schema can be created also for preparation)
5. Run 'ddl_bronze' sql file to pgadmin query tool to create the database tables
6. Run 'load_procedure_bronze' sql file to pgadmin query tool to bulk upload all csv data from the 'datasets' folder
7. Run 'CALL bronze.load_bronze()' to pgadmin query tool

Note: Use 'Select * from [TABLE_NAME]' to check if the data from the csv file was loaded correctly