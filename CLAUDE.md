# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckLake-based e-commerce analytics project that analyzes synthetic e-commerce data using the DuckLake lakehouse format. DuckLake provides a robust data management layer on top of DuckDB with features like schema evolution, time travel, and transactional consistency. The main objective is to identify the highest-selling product by total sales value for each country, including comprehensive metrics like ratings, customer demographics, and sales volumes.

## Commands

### Running the Analysis
```bash
python main.py
```

### Installing Dependencies
```bash
pip install -r requirements.txt
```

## Data Architecture

The project uses DuckLake as the lakehouse format, managing 5 relational tables that are initially loaded from Parquet files in `archive/parquet/`:

- **customers** (~2M rows): Customer demographics and signup info
- **products** (~20K rows): Product catalog with pricing and inventory
- **orders** (~8M rows): Order transactions and shipping details
- **order_items** (~20M rows): Individual items within orders
- **product_reviews** (~4M rows): Customer ratings and reviews

### DuckLake Features
- **Transactional consistency**: All operations are ACID-compliant
- **Schema evolution**: Tables can be modified without breaking existing queries
- **Time travel**: Query historical versions of data using snapshot functionality
- **Metadata catalog**: Centralized metadata management via DuckDB or external databases

### Key Relationships
- Customers → Orders (1:many)
- Orders → Order Items (1:many) 
- Products → Order Items (1:many)
- Products → Product Reviews (1:many)
- Customers → Product Reviews (1:many)

## Core Query Logic

The main analysis uses a complex CTE-based SQL query that:
1. Calculates total sales and quantities per product by country
2. Computes average ratings from product reviews
3. Analyzes customer demographics (gender distribution, unique customers)
4. Ranks products by sales within each country
5. Limits results to top 10 countries by sales volume

## File Structure

- `main.py` - Main execution script with DuckLake integration
- `requirements.txt` - Python dependencies (duckdb>=1.3.0, pandas, numpy)
- `archive/parquet/` - Source data files in Parquet format
- `ecommerce_analytics.ducklake` - DuckLake database file (auto-created)
- `ecommerce_analytics.ducklake.files/` - DuckLake data directory (auto-created)
- `README.md` - Detailed project documentation in Italian

## Technical Notes

- **DuckLake Integration**: Uses DuckLake extension for enhanced data management
- **Automatic Table Creation**: Tables are created from Parquet files on first run
- **Persistent Storage**: Data is stored in DuckLake format for future access
- **Version Management**: DuckDB version 1.3.0+ required for DuckLake extension
- **Query Performance**: Leverages DuckLake's optimized storage and indexing
- **Results Format**: Returns pandas DataFrame for easy manipulation/display
- **ACID Compliance**: All operations benefit from DuckLake's transactional guarantees