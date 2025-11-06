# azure-excel-etl-pipeline

# Azure Excel ETL Pipeline

## Overview
This project demonstrates a Python-based Azure Function that automates the end-to-end ETL process for Excel files stored in Azure Blob Storage.  
The function extracts, cleans, and uploads structured data into an Azure SQL Database, while also managing and archiving associated image files found inside the Excel sheets.

## Key Features
- Automatically retrieves Excel files from Azure Blob Storage  
- Extracts and cleans data using Pandas  
- Uploads processed data into Azure SQL Database via SQLAlchemy  
- Extracts embedded images and uploads them to Blob Storage  
- Archives previous image versions with timestamp tracking  
- Adds audit columns (Create_date, Last_modified, Created_by)  
- Includes structured logging and error handling  

## Tech Stack
| Tool / Library | Purpose |
|-----------------|----------|
| Python 3.10+ | Core programming language |
| Azure Functions | Serverless runtime for ETL trigger |
| Azure Blob Storage | File and image storage |
| Azure SQL Database | Structured data storage |
| Pandas | Data cleaning and transformation |
| SQLAlchemy | Database connection and ORM |
| OpenPyXL / openpyxl-image-loader | Excel and image parsing |
| Pytz | Timezone handling |
| Logging | Operational monitoring and debugging |

## Architecture
1. Excel File (Blob Storage)
2. Azure Function Trigger (Python)
  2.1 Extract Data (Pandas)
  2.2 Clean & Transform
  2.3 Upload to Azure SQL (SQLAlchemy)
3. Extract Images → Upload to Blob / Archive
