# Azure Excel ETL Pipeline

A serverless **Azure Function** that automates extracting, transforming, and loading Excel data from Azure Blob Storage into Azure SQL Database, with embedded image extraction and archival.

## Architecture

```
┌────────────────────┐     ┌──────────────────────────────┐     ┌──────────────────┐
│  Azure Blob        │     │     Azure Function           │     │  Azure SQL       │
│  Storage           │────▶│                              │────▶│  Database        │
│  (Excel files)     │     │  1. Extract data (Pandas)    │     │  (clean data)    │
│                    │     │  2. Clean & transform        │     │                  │
└────────────────────┘     │  3. Upload via SQLAlchemy    │     └──────────────────┘
                           │  4. Extract images           │
                           └──────────┬───────────────────┘
                                      │
                                      ▼
                           ┌──────────────────────┐
                           │  Azure Blob Storage   │
                           │  (images + archive)   │
                           └──────────────────────┘
```

## Key Features

- Automatically retrieves Excel files from Azure Blob Storage
- Extracts and cleans data using Pandas
- Uploads processed data into Azure SQL Database via SQLAlchemy
- Extracts embedded images and uploads them to Blob Storage
- Archives previous image versions with timestamp tracking
- Adds audit columns (`Create_date`, `Last_modified`, `Created_by`)
- Structured logging and error handling

## Tech Stack

| Tool / Library | Purpose |
|---------------|---------|
| Python 3.10+ | Core language |
| Azure Functions | Serverless ETL runtime |
| Azure Blob Storage | File and image storage |
| Azure SQL Database | Structured data storage |
| Pandas | Data cleaning and transformation |
| SQLAlchemy | Database connection and ORM |
| OpenPyXL | Excel and image parsing |
| Pytz | Timezone handling |

## Project Structure

```
├── main.py          # Azure Function with ETL logic
└── README.md
```

## How It Works

1. Azure Function triggers on new Excel file in Blob Storage
2. Downloads and parses Excel using OpenPyXL + Pandas
3. Cleans data, adds audit columns
4. Uploads structured data to Azure SQL via SQLAlchemy
5. Extracts embedded images → uploads to separate Blob container
6. Archives old images with timestamps
