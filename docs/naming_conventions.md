# **Naming Conventions**

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data warehouse.

## **Table of Contents**

1. [General Principles](#general-principles)  
2. [Table Naming Conventions](#table-naming-conventions)  
   - [Bronze Rules](#bronze-rules)  
   - [Silver Rules](#silver-rules)  
   - [Gold Rules](#gold-rules)  
3. [Column Naming Conventions](#column-naming-conventions)  
   - [Surrogate Keys](#surrogate-keys)  
   - [Metadata Columns](#metadata-columns)  
4. [Stored Procedure Naming Conventions](#stored-procedure-naming-conventions)
5. [Special Naming Patterns](#special-naming-patterns)

---

## **General Principles**

- **Naming Conventions**: Use `snake_case`, with lowercase letters and underscores (`_`) to separate words.  
- **Language**: Use English for all names.  
- **Avoid Reserved Words**: Do not use SQL reserved words as object names.

---

## **Table Naming Conventions**

All tables must be placed in their respective **schema layer** (`bronze`, `silver`, `gold`, `ref`, `public`) and follow the established patterns based on the layer and purpose.

### **Bronze Rules**

- Schema: `bronze`  
- Table names must match the original names from the source system without renaming.  
- **Format**: `bronze.<sourcesystem>_<entity>`  
  - `<sourcesystem>`: Source system name (e.g., `envi`, `csv`, `csr`, `hr`, `econ`)  
  - `<entity>`: Exact name from the source dataset  
  - Examples:
    - `bronze.envi_water_consumption` → Water consumption from the ENVI dataset
    - `bronze.hr_demographics` → Employee demographics from HR system
    - `bronze.csr_activity` → CSR activities from CSR system
    - `bronze.csv_energy_records` → Energy production records from CSV files
    - `bronze.econ_value` → Economic value data from finance system

### **Silver Rules**

- Schema: `silver`  
- Table names must also match the original names from the source system.  
- **Format**: `silver.<sourcesystem>_<entity>`  
  - Examples:
    - `silver.envi_water_consumption` → Cleaned water consumption data
    - `silver.hr_demographics` → Validated employee demographics
    - `silver.csr_activity` → Standardized CSR activity data
    - `silver.wa_id_mapping` → Special mapping table for water abstraction IDs

### **Gold Rules**

- Schema: `gold`  
- The gold layer primarily uses **views** rather than physical tables to create business-ready data models.
- **Format**: `gold.<category>_<entity>` or `gold.vw_<domain>_<entity>`  
  - `<category>`: `dim`, `fact`, `vw`, or `report`  
  - `<entity>`: Business-aligned view name  
  - Examples:
    - `gold.dim_employee_descriptions` → Employee dimension view
    - `gold.vw_environment_water_abstraction` → Environmental water abstraction view
    - `gold.dim_program_descriptions` → CSR program dimension view

#### **Glossary of Category Patterns**

| Pattern     | Meaning             | Example(s)                         |
|-------------|---------------------|------------------------------------|
| `dim_`      | Dimension view      | `gold.dim_employee_descriptions`   |
| `fact_`     | Fact view           | `gold.fact_environmental_metrics`  |
| `vw_`       | Business view       | `gold.vw_environment_water_abstraction` |
| `report_`   | Reporting view      | `gold.report_monthly_csr_summary`  |

### **Reference Schema Rules**

- Schema: `ref`  
- Contains master data and reference tables used across all layers.
- **Format**: `ref.<entity>` or `ref.ref_<entity>`  
  - Examples:
    - `ref.company_main` → Master company information
    - `ref.ref_emission_factors` → Emission calculation factors
    - `ref.ref_power_plants` → Power plant reference data
    - `ref.expenditure_type` → Expenditure type lookup

### **Public Schema Rules**

- Schema: `public`  
- Contains application-facing tables for user management and system operations.
- **Format**: `public.<entity>`  
  - Examples:
    - `public.account` → User account management
    - `public.user_profile` → User profile information
    - `public.audit_trail` → System audit logging
    - `public.roles` → User role definitions
    - `public.status` → System status codes

---

## **Column Naming Conventions**

### **Primary Keys**

The project uses different primary key naming patterns depending on the layer and purpose:

#### **Bronze and Silver Layers**
- Use descriptive primary keys that match the source system
- **Format**: `<entity>_id` or `<prefix>_id`
- Examples:
  - `employee_id` → Employee identifier
  - `wc_id` → Water consumption record ID
  - `csr_id` → CSR activity ID
  - `dc_id` → Diesel consumption ID
  - `ec_id` → Electric consumption ID

#### **Gold Layer (Dimension Tables)**
- Use the suffix `_key` for surrogate keys in dimension tables
- **Format**: `<entity>_key`
- Examples:
  - `employee_key` → Employee dimension key
  - `company_key` → Company dimension key

#### **Public Schema**
- Use descriptive identifiers for application tables
- **Format**: `<entity>_id`
- Examples:
  - `account_id` → User account identifier
  - `role_id` → Role identifier
  - `status_id` → Status identifier

### **Foreign Keys**

Foreign keys follow these patterns:
- **Reference Keys**: Match the referenced table's primary key name
- **Company References**: `company_id` (consistent across all tables)
- **Descriptive Keys**: Include the referenced entity name
- Examples:
  - `company_id` → References company tables
  - `power_plant_id` → References power plant data
  - `employee_id` → References employee data
  - `account_id` → References user accounts

### **Metadata Columns**

Standard metadata tracking columns must be named:
- **Creation Timestamps**: 
  - `date_created` → When the record was created
  - `profile_created` → When the profile was created
  - `created_at` → Generic creation timestamp
- **Update Timestamps**:
  - `date_updated` → When the record was last updated
  - `profile_updated` → When the profile was last updated
  - `updated_at` → Generic update timestamp
- **Status Tracking**:
  - `account_status` → Account status
  - `employment_status` → Employment status
  - `operational_status` → Operational status

### **Data Columns**

Common data column naming patterns:
- **Temporal Columns**: `year`, `quarter`, `month`, `production_date`
- **Measurement Columns**: `volume`, `amount`, `energy_produced`
- **Unit Columns**: `unit_of_measurement`, `unit`
- **Descriptive Columns**: `description`, `remarks`, `project_remarks`
- **Name Columns**: `first_name`, `last_name`, `company_name`, `site_name`
- **Address Columns**: `address`, `site_address`, `city_town`, `province`

### **Boolean and Status Columns**

- **Boolean Indicators**: Use descriptive names
  - `p_np` → Professional/Non-professional indicator
- **Status Codes**: Use `_status` suffix
  - `account_status`, `employment_status`
- **Type Classifications**: Use `_type` suffix
  - `expenditure_type`, `action_type`, `incident_type`

---

## **Stored Procedure and Function Naming Conventions**

The project uses both stored procedures for data loading and functions for data processing and calculations.

### **Stored Procedures**

All stored procedures for loading data into each layer must reside in the corresponding schema and follow this pattern:  
**`<layer>.load_<entity>_<layer>`**

- `<layer>`: `bronze`, `silver`, `gold`, or `ref`  
- `<entity>`: Source or business entity name  
- Examples:
  - `bronze.load_envi_bronze()` → Loads environmental data into the bronze layer  
  - `silver.load_hr_silver()` → Loads processed HR data into the silver layer  
  - `bronze.load_csv_bronze()` → Loads CSV energy data into the bronze layer
  - `ref.load_ref()` → Loads reference data

### **Functions**

Functions are primarily used in the gold layer for business calculations and data processing:
**`gold.func_<purpose>_<entity>`**

- `<purpose>`: Describes the function's purpose (e.g., `fact`, `summary`, `calculation`)
- `<entity>`: Business domain or specific calculation
- Examples:
  - `gold.func_employee_summary()` → Employee summary calculations
  - `gold.func_fact_energy()` → Energy production fact calculations
  - `gold.func_environment_water_abstraction_by_year()` → Water abstraction analysis
  - `gold.func_co2_equivalence_per_metric()` → CO2 emission calculations

---

## **Special Naming Patterns**

### **Staging Tables**

Staging tables are used for intermediate data processing and follow this pattern:
**`<layer>.<entity>_staging`**

- Examples:
  - `bronze.hr_parental_leave_staging` → Staging table for parental leave data
  - `bronze.hr_training_staging` → Staging table for training data
  - `bronze.hr_safety_workdata_staging` → Staging table for safety work data

### **Mapping Tables**

Mapping tables are used to maintain relationships between different data sources:
**`<layer>.<entity>_mapping`**

- Examples:
  - `silver.wa_id_mapping` → Mapping table for water abstraction IDs between bronze and silver

### **View Naming in Gold Layer**

The gold layer primarily uses views and follows these patterns:
- **Dimension Views**: `dim_<entity>_<purpose>`
- **Business Views**: `vw_<domain>_<entity>`
- **Fact Views**: `fact_<entity>_<metrics>`

### **File Naming Conventions**

SQL script files follow these patterns:
- **DDL Scripts**: `ddl_<layer>.sql` or `ddl_<purpose>.sql`
- **Load Procedures**: `load_procedure_<layer>.sql` or `proc_load_<entity>_<layer>.sql`
- **Functions**: `functions_<layer>.sql` or `<entity>_functions_<layer>.sql`
- **Views**: `<entity>_<layer>_views.sql`
