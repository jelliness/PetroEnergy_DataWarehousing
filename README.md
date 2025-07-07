# PetroEnergy Data Warehousing

A comprehensive data warehousing solution for PetroEnergy Resources Corp and its subsidiaries, implementing a medallion architecture (Bronze-Silver-Gold) to manage and analyze environmental, HR, economic, and CSR data across multiple renewable energy companies.

## 🏗️ Architecture Overview

This data warehouse follows the **Medallion Architecture** pattern:

- **Bronze Layer**: Raw data ingestion from source systems
- **Silver Layer**: Cleaned, validated, and standardized data
- **Gold Layer**: Business-ready data models for analytics and reporting
- **Public Schema**: Application-facing tables for user management and audit trails

## 📊 Data Domains

### 1. Environmental Data (ENVI)
- **Water Management**: Consumption, withdrawal, abstraction, and discharge tracking
- **Energy Consumption**: Diesel fuel and electricity usage monitoring
- **Waste Management**: Hazardous and non-hazardous waste generation and disposal
- **Natural Resources**: Tracking of natural water sources and company properties

### 2. Human Resources (HR)
- **Demographics**: Employee personal information and workforce composition
- **Safety & Health**: Occupational safety incidents and health metrics
- **Training**: Employee development and certification programs
- **Tenure & Leave**: Employment history and parental leave tracking

### 3. Economic Data (ECON)
- **Financial Performance**: Revenue streams and income tracking
- **Capital Expenditures**: Investment and operational spending
- **Provider Payments**: Vendor and supplier payment records

### 4. Corporate Social Responsibility (CSR)
- **Programs**: Community engagement and social impact initiatives
- **Projects**: Detailed project execution and outcomes
- **Activities**: Day-to-day CSR activities and reporting

### 5. Energy Production (CSV)
- **Power Generation**: Renewable energy production records
- **Plant Operations**: Solar and wind farm performance data

## 🏢 Company Structure

The data warehouse manages data for the following companies:

- **PERC**: PetroEnergy Resources Corp (Parent Company)
- **PGEC**: PetroGreen Energy Corp
- **PSC**: PetroSolar Corp
- **PWEI**: PetroWind Energy Inc.
- **MGI**: Maibarara Geothermal Inc.
- **RGEC**: Renewable Green Energy Corp

## 📁 Project Structure

```
PetroEnergy_DataWarehousing/
├── datasets/                    # Source data files
│   ├── company_reference/       # Company master data and reference tables
│   ├── source_csr/             # CSR activity and program data
│   ├── source_csv/             # Energy production records
│   ├── source_econ/            # Economic and financial data
│   ├── source_envi/            # Environmental monitoring data
│   └── source_hr/              # Human resources data
├── scripts/                     # Database scripts
│   ├── bronze/                 # Bronze layer DDL and DML scripts
│   ├── silver/                 # Silver layer transformation scripts
│   ├── gold/                   # Gold layer business model scripts
│   ├── public/                 # Public schema for application layer
│   └── ref/                    # Reference data scripts
├── tests/                      # Data quality and validation tests
├── docs/                       # Documentation and data catalogs
└── database/                   # Database backup and restore scripts
```

## 🔧 Setup and Installation

### Prerequisites
- PostgreSQL 12+
- Administrative access to create databases and schemas

### Installation Steps

1. **Initialize Database**
   ```sql
   -- Run the main initialization script
   \i scripts/init_database.sql
   ```

2. **Create Schemas**
   ```sql
   -- Create public schema tables
   \i scripts/public/ddl_public.sql
   
   -- Create reference tables
   \i scripts/ref/ddl.sql
   \i scripts/ref/load_ref.sql
   ```

3. **Build Data Layers**
   ```sql
   -- Bronze layer
   \i scripts/bronze/*/ddl_bronze.sql
   
   -- Silver layer  
   \i scripts/silver/*/ddl_silver.sql
   
   -- Gold layer
   \i scripts/gold/*/ddl_gold.sql
   ```

## 📚 Data Dictionary

### Public Schema Tables

#### `public.roles`
User role definitions for system access control.

| Column | Type | Description |
|--------|------|-------------|
| role_id | VARCHAR(3) | Primary key, role identifier |
| role_name | VARCHAR(30) | Descriptive name of the role |

#### `public.status`
System status codes for various operational states.

| Column | Type | Description |
|--------|------|-------------|
| status_id | VARCHAR(3) | Primary key, status identifier |
| status_name | VARCHAR(30) | Descriptive name of the status |

#### `public.account`
User account management and authentication.

| Column | Type | Description |
|--------|------|-------------|
| account_id | CHAR(26) | Primary key, unique account identifier |
| email | VARCHAR(254) | User email address |
| account_role | VARCHAR(3) | Foreign key to roles table |
| power_plant_id | VARCHAR(10) | Associated power plant |
| company_id | VARCHAR(10) | Associated company |
| account_status | VARCHAR(10) | Current account status |
| date_created | TIMESTAMP | Account creation timestamp |
| date_updated | TIMESTAMP | Last update timestamp |
| password | TEXT | Encrypted password |

#### `public.user_profile`
Extended user profile information.

| Column | Type | Description |
|--------|------|-------------|
| emp_id | VARCHAR(20) | Employee ID (if applicable) |
| account_id | CHAR(26) | Primary key, links to account |
| first_name | VARCHAR(50) | User's first name |
| last_name | VARCHAR(50) | User's last name |
| middle_name | VARCHAR(50) | User's middle name |
| suffix | VARCHAR(5) | Name suffix (Jr., Sr., etc.) |
| contact_number | VARCHAR(20) | Phone number |
| address | TEXT | Physical address |
| birthdate | DATE | Date of birth |
| gender | VARCHAR(10) | Gender |
| profile_created | TIMESTAMP | Profile creation timestamp |
| profile_updated | TIMESTAMP | Last profile update |

#### `public.audit_trail`
System audit logging for data changes.

| Column | Type | Description |
|--------|------|-------------|
| audit_id | VARCHAR(20) | Primary key, unique audit record ID |
| account_id | CHAR(26) | User who performed the action |
| target_table | VARCHAR(20) | Table that was modified |
| record_id | VARCHAR(20) | ID of the affected record |
| action_type | VARCHAR(10) | Type of action (INSERT, UPDATE, DELETE) |
| old_value | TEXT | Previous value (for updates) |
| new_value | TEXT | New value |
| audit_timestamp | TIMESTAMP | When the action occurred |
| description | TEXT | Additional details about the action |

#### `public.checker_status_log`
Data quality checking and validation log.

| Column | Type | Description |
|--------|------|-------------|
| cs_id | VARCHAR(20) | Primary key, check status ID |
| checker_id | CHAR(26) | User who performed the check |
| record_id | VARCHAR(20) | Record being checked |
| status_id | VARCHAR(3) | Status of the check |
| status_timestamp | TIMESTAMP | When the check was performed |
| remarks | TEXT | Additional comments |

### Reference Data Tables

#### `company_main`
Master company information and hierarchy.

| Column | Type | Description |
|--------|------|-------------|
| company_id | VARCHAR(10) | Primary key, company identifier |
| company_name | VARCHAR(100) | Full company name |
| parent_company_id | VARCHAR(10) | Parent company (if subsidiary) |
| address | TEXT | Company headquarters address |

#### `ref_power_plants`
Power plant facility information.

| Column | Type | Description |
|--------|------|-------------|
| power_plant_id | VARCHAR(10) | Primary key, plant identifier |
| company_id | VARCHAR(10) | Owning company |
| site_name | VARCHAR(100) | Plant name |
| site_address | TEXT | Plant location |
| city_town | VARCHAR(50) | City/Town |
| province | VARCHAR(50) | Province |
| country | VARCHAR(50) | Country |
| zip | VARCHAR(10) | Postal code |
| ef_id | VARCHAR(10) | Emission factor ID |

#### `ref_emission_factors`
Emission calculation factors for different energy sources.

| Column | Type | Description |
|--------|------|-------------|
| ef_id | VARCHAR(10) | Primary key, emission factor ID |
| energy_source | VARCHAR(50) | Type of energy source |
| ef_value | DECIMAL(10,6) | Emission factor value |
| unit | VARCHAR(20) | Unit of measurement |

### Environmental Data Tables

#### `envi_water_consumption`
Water usage tracking across facilities.

| Column | Type | Description |
|--------|------|-------------|
| wc_id | VARCHAR(20) | Primary key, consumption record ID |
| company_id | VARCHAR(10) | Company identifier |
| year | INTEGER | Year of measurement |
| quarter | VARCHAR(2) | Quarter (Q1, Q2, Q3, Q4) |
| volume | DECIMAL(12,2) | Volume consumed |
| unit_of_measurement | VARCHAR(20) | Unit (cubic meter, liters, etc.) |

#### `envi_diesel_consumption`
Diesel fuel usage monitoring.

| Column | Type | Description |
|--------|------|-------------|
| dc_id | VARCHAR(20) | Primary key, consumption record ID |
| company_id | VARCHAR(10) | Company identifier |
| year | INTEGER | Year of measurement |
| quarter | VARCHAR(2) | Quarter |
| volume | DECIMAL(12,2) | Volume consumed |
| unit_of_measurement | VARCHAR(20) | Unit |

#### `envi_electric_consumption`
Electricity usage tracking.

| Column | Type | Description |
|--------|------|-------------|
| ec_id | VARCHAR(20) | Primary key, consumption record ID |
| company_id | VARCHAR(10) | Company identifier |
| year | INTEGER | Year of measurement |
| quarter | VARCHAR(2) | Quarter |
| volume | DECIMAL(12,2) | Volume consumed |
| unit_of_measurement | VARCHAR(20) | Unit (kWh, MWh, etc.) |

#### `envi_hazard_waste_generated`
Hazardous waste generation tracking.

| Column | Type | Description |
|--------|------|-------------|
| hwg_id | VARCHAR(20) | Primary key, waste record ID |
| company_id | VARCHAR(10) | Company identifier |
| year | INTEGER | Year of measurement |
| quarter | VARCHAR(2) | Quarter |
| volume | DECIMAL(12,2) | Volume generated |
| unit_of_measurement | VARCHAR(20) | Unit |

### Human Resources Data Tables

#### `hr_demographics`
Employee demographic information.

| Column | Type | Description |
|--------|------|-------------|
| employee_id | VARCHAR(20) | Primary key, employee identifier |
| gender | VARCHAR(1) | Gender (M/F) |
| birthdate | DATE | Date of birth |
| position_id | VARCHAR(10) | Position/Role code |
| p_np | VARCHAR(2) | Professional/Non-professional indicator |
| company_id | VARCHAR(10) | Employing company |
| employment_status | VARCHAR(20) | Employment status |

#### `hr_training`
Employee training and development records.

| Column | Type | Description |
|--------|------|-------------|
| training_id | VARCHAR(20) | Primary key, training record ID |
| employee_id | VARCHAR(20) | Employee identifier |
| training_type | VARCHAR(50) | Type of training |
| training_date | DATE | Date of training |
| duration_hours | INTEGER | Training duration |
| certification | VARCHAR(100) | Certification received |

#### `hr_occupational_safety_health`
Workplace safety and health incidents.

| Column | Type | Description |
|--------|------|-------------|
| osh_id | VARCHAR(20) | Primary key, incident record ID |
| company_id | VARCHAR(10) | Company identifier |
| year | INTEGER | Year of incident |
| incident_type | VARCHAR(50) | Type of incident |
| incident_count | INTEGER | Number of incidents |
| severity_level | VARCHAR(20) | Severity classification |

### Economic Data Tables

#### `econ_value`
Financial performance and revenue data.

| Column | Type | Description |
|--------|------|-------------|
| year | INTEGER | Primary key, financial year |
| electricity_sales | DECIMAL(15,2) | Revenue from electricity sales |
| oil_revenues | DECIMAL(15,2) | Revenue from oil operations |
| other_revenues | DECIMAL(15,2) | Other revenue sources |
| interest_income | DECIMAL(15,2) | Interest income |
| share_in_net_income_of_associate | DECIMAL(15,2) | Associate company income |
| miscellaneous_income | DECIMAL(15,2) | Other miscellaneous income |

#### `econ_expenditures`
Operational and capital expenditures.

| Column | Type | Description |
|--------|------|-------------|
| expenditure_id | VARCHAR(20) | Primary key, expenditure record ID |
| company_id | VARCHAR(10) | Company identifier |
| year | INTEGER | Year of expenditure |
| category | VARCHAR(50) | Expenditure category |
| amount | DECIMAL(15,2) | Amount spent |
| description | TEXT | Description of expenditure |

### CSR Data Tables

#### `csr_activity`
Corporate social responsibility activities.

| Column | Type | Description |
|--------|------|-------------|
| csr_id | VARCHAR(20) | Primary key, CSR activity ID |
| company_id | VARCHAR(10) | Company identifier |
| project_id | VARCHAR(20) | Associated project |
| ac_year | INTEGER | Activity year |
| csr_report | DECIMAL(12,2) | Reported value/amount |
| project_expenses | DECIMAL(12,2) | Project expenses |
| project_remarks | TEXT | Additional remarks |

#### `csr_programs`
CSR program definitions and categories.

| Column | Type | Description |
|--------|------|-------------|
| program_id | VARCHAR(20) | Primary key, program identifier |
| program_name | VARCHAR(100) | Program name |
| program_category | VARCHAR(50) | Category of program |
| description | TEXT | Program description |
| target_beneficiaries | VARCHAR(100) | Target beneficiary groups |

### Energy Production Data Tables

#### `csv_energy_records`
Renewable energy production records.

| Column | Type | Description |
|--------|------|-------------|
| record_id | VARCHAR(20) | Primary key, production record ID |
| power_plant_id | VARCHAR(10) | Power plant identifier |
| production_date | DATE | Date of production |
| energy_produced | DECIMAL(12,2) | Energy produced |
| unit_of_measurement | VARCHAR(20) | Unit (kWh, MWh, etc.) |
| weather_conditions | VARCHAR(50) | Weather conditions |
| operational_status | VARCHAR(20) | Plant operational status |

## 🔍 Data Quality and Testing

The project includes comprehensive data quality checks:

- **Automated Quality Checks**: SQL scripts that validate data integrity
- **Silver Layer Validation**: Ensures data cleaning and standardization
- **Gold Layer Testing**: Validates business rules and calculations
- **Public Schema Testing**: Verifies application layer data consistency

Quality check scripts are located in the `/tests/` directory, organized by data layer.

## 📈 Usage Examples

### Basic Data Retrieval
```sql
-- Get company hierarchy
SELECT 
    c.company_name,
    p.company_name as parent_company
FROM company_main c
LEFT JOIN company_main p ON c.parent_company_id = p.company_id;

-- Water consumption by company and year
SELECT 
    company_id,
    year,
    SUM(volume) as total_consumption
FROM envi_water_consumption
GROUP BY company_id, year
ORDER BY year DESC, company_id;
```

### Analytics Queries
```sql
-- CSR spending by program category
SELECT 
    cp.program_category,
    SUM(ca.project_expenses) as total_expenses
FROM csr_activity ca
JOIN csr_programs cp ON ca.project_id = cp.program_id
GROUP BY cp.program_category;

-- Employee demographics by company
SELECT 
    company_id,
    gender,
    COUNT(*) as employee_count
FROM hr_demographics
GROUP BY company_id, gender;
```

## 📄 License

This project is proprietary to PetroEnergy Resources Corp and its subsidiaries.

## 👥 Contributing

For internal development teams only. Please follow the established naming conventions and data quality standards outlined in `/docs/naming_conventions.md`.

## 📞 Support

For technical support and questions, please contact the Data Engineering team.

---

*Last updated: July 2025*