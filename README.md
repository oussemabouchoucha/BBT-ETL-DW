Data Warehouse pour BBT
Description

This project outlines an Extract, Transform, Load (ETL) process for data warehousing. It leverages Python scripts to extract data, perform transformations, and load the prepared data into a data warehouse or data lake.

ETL Steps

Data Extraction (etl_extract.py):

This script extracts data from CSV files.
It's crucial to configure connection details and data retrieval logic within this script.
Data Transformation (etl_transformation):

This directory houses various Python scripts for data transformations.
audit_report.py: Generates a report detailing potential data quality issues encountered during extraction.
data_cleaning.py: Cleans and prepares data, including handling missing values, inconsistencies, and formatting errors.
add_columns.py: Adds new calculated columns or derived attributes based on business logic.
sales_tax.py: Calculates and applies sales tax based on predefined rules or tax rates.
sales_currency.py: Converts sales data to a consistent currency format if necessary.
etl_gold/map_cols.py: (New) Maps columns from the source data to the target data warehouse schema. This ensures proper data alignment and understanding.
Data Loading (etl_load.py):

This script loads the transformed data into the target data warehouse or data lake.
Configure connection details and data insertion logic within this script.
Data Warehouse Design Considerations

SCD Type 2 in Dimensions: The schema supports historical tracking of dimension changes using StartDate, EndDate, and IsCurrent columns (e.g., in DimCustomers and DimProducts). This allows for analyzing data at specific points in time.
Fact Table: This table holds all Key Performance Indicators (KPIs) like AttractivenessIndex, CustomerValue, and ProductSalesStatus.
Date Dimension: Enables filtering and aggregation based on various time periods.
Surrogate Keys: The fact table utilizes surrogate keys (e.g., SalesID) to enhance performance and data integration.
Prerequisites

Python (Recommended version: [Specify your preferred version])
Python libraries (List any specific libraries required)
Access to data sources (Specify the types and credentials if needed)
Installation

Clone this repository:

Bash
git clone https://github.com/Hajjej-adam/spark_project.git
Use code with caution.

Install required Python libraries (Replace with specific commands for your libraries):

Bash
pip install -r requirements.txt
Use code with caution.

Usage

Configure connection details in the relevant scripts (e.g., database credentials for extraction, target data warehouse connection for loading).

Review and customize the data transformation logic within the etl_transformation scripts to meet your specific needs.

Run the ETL process by executing the scripts in the correct sequence:

python etl_extract.py
python etl_transformation/data_cleaning.py (and other transformation scripts as needed)
python etl_load.py