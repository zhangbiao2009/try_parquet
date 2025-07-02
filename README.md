# OLAP Sample Data Generator

This project generates sample OLAP (Online Analytical Processing) data and saves it to both Parquet and CSV files. The data follows a star schema design with fact and dimension tables, making it suitable for analytical queries and business intelligence applications.

## Overview

The generated dataset includes:

### Dimension Tables
- **Time Dimension** (`dim_time.parquet`): Date hierarchy with year, quarter, month, week, day
- **Geography Dimension** (`dim_geography.parquet`): Regional hierarchy with region, country, city
- **Product Dimension** (`dim_product.parquet`): Product hierarchy with category, subcategory, product
- **Customer Dimension** (`dim_customer.parquet`): Customer information and segmentation

### Fact Table
- **Sales Fact** (`fact_sales.parquet`): Sales transactions with measures like quantity, sales amount, cost, profit

## Features

- **Realistic Data**: Generated with realistic patterns, seasonal trends, and business logic
- **Star Schema**: Optimized for OLAP queries with proper dimension and fact table structure
- **Parquet Format**: Efficient columnar storage format ideal for analytics
- **Hierarchical Dimensions**: Support for drill-down and roll-up operations
- **Sample Analyses**: Includes analysis scripts demonstrating various OLAP operations

## Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Generate OLAP Data

Run the data generator to create sample OLAP data:

```bash
python generate_olap_data.py
```

This will create two directories containing the data in different formats:

**Parquet format** (`olap_data` directory):
- `dim_time.parquet` - Time dimension (1,827 records)
- `dim_geography.parquet` - Geography dimension (~63 records)
- `dim_product.parquet` - Product dimension (~150 records)
- `dim_customer.parquet` - Customer dimension (1,000 records)
- `fact_sales.parquet` - Sales fact table (50,000 records)

**CSV format** (`csv_data` directory):
- `dim_time.csv` - Time dimension
- `dim_geography.csv` - Geography dimension
- `dim_product.csv` - Product dimension
- `dim_customer.csv` - Customer dimension
- `fact_sales.csv` - Sales fact table

### Analyze the Data

Run the analysis script to see sample OLAP queries:

```bash
python analyze_olap_data.py
```

This will perform various analytical queries including:
- Sales trends by time periods
- Geographic analysis
- Product performance analysis
- Customer segmentation
- Multidimensional analysis

## Data Schema

### Time Dimension
- `date_key`: Primary key
- `date`: Actual date
- `year`, `quarter`, `month`, `day`: Date components
- `day_of_week`, `week_of_year`: Week information
- `is_weekend`: Weekend flag
- `fiscal_year`: Fiscal year

### Geography Dimension
- `geography_key`: Primary key
- `city`, `country`, `region`: Geographic hierarchy

### Product Dimension
- `product_key`: Primary key
- `sku`, `product_name`: Product identifiers
- `product_type`, `subcategory`, `category`: Product hierarchy
- `unit_cost`, `unit_price`: Pricing information

### Customer Dimension
- `customer_key`: Primary key
- `customer_id`: Business key
- `customer_type`: Segmentation (Individual, Small Business, Enterprise)
- `registration_date`: Customer acquisition date

### Sales Fact
- `sales_key`: Primary key
- `date_key`, `geography_key`, `product_key`, `customer_key`: Foreign keys
- `quantity`: Units sold
- `unit_price`, `unit_cost`: Per-unit amounts
- `gross_sales`, `total_cost`, `profit`: Calculated measures

## Use Cases

This sample data is perfect for:
- Learning OLAP concepts and cube design
- Testing BI tools and data visualization software
- Prototyping analytical applications
- Benchmarking query performance
- Training on dimensional modeling

## Customization

You can modify the data generation parameters in `generate_olap_data.py`:
- Date ranges for time dimension
- Geographic regions and cities
- Product categories and hierarchies
- Number of customers and sales records
- Business rules and data patterns

## File Formats

The data is available in two formats:

### Parquet Format
- Columnar storage for fast analytical queries
- Efficient compression
- Schema evolution support
- Wide ecosystem support (Spark, Pandas, etc.)
- Ideal for data warehouses and big data analytics

### CSV Format  
- Universal compatibility with most tools and applications
- Human-readable format
- Easy to import into spreadsheets and databases
- Good for data exchange and initial exploration

## Next Steps

With this sample data, you can:
1. Import into your favorite BI tool (Tableau, Power BI, etc.)
2. Create OLAP cubes using tools like Apache Kylin or Microsoft SSAS
3. Build dashboards and reports
4. Practice SQL analytical functions
5. Experiment with different visualization techniques
