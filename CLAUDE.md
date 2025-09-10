# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB-based e-commerce analytics project that analyzes synthetic e-commerce data in Parquet format. The main objective is to identify the highest-selling product by total sales value for each country, including comprehensive metrics like ratings, customer demographics, and sales volumes.

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

The project works with 5 relational tables stored as Parquet files in `archive/parquet/`:

- **customers** (~2M rows): Customer demographics and signup info
- **products** (~20K rows): Product catalog with pricing and inventory
- **orders** (~8M rows): Order transactions and shipping details
- **order_items** (~20M rows): Individual items within orders
- **product_reviews** (~4M rows): Customer ratings and reviews

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

- `main.py` - Main execution script with DuckDB query
- `requirements.txt` - Python dependencies (duckdb, pandas, numpy)
- `archive/parquet/` - Data files in Parquet format
- `README.md` - Detailed project documentation in Italian

## Technical Notes

- All data processing is done in-memory using DuckDB's `read_parquet()` function
- No traditional database setup required - queries run directly on Parquet files
- Results are returned as pandas DataFrame for easy manipulation/display
- The query uses window functions (RANK()) for efficient top-N selection per group