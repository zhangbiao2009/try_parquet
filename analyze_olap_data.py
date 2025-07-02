#!/usr/bin/env python3
"""
Analyze the generated OLAP data from Parquet files.
This script demonstrates various OLAP-style queries on the sample data.
"""

import pandas as pd
from pathlib import Path

def load_data():
    """Load all dimension and fact tables from Parquet files."""
    data_dir = Path('olap_data')
    
    tables = {}
    tables['time'] = pd.read_parquet(data_dir / 'dim_time.parquet')
    tables['geography'] = pd.read_parquet(data_dir / 'dim_geography.parquet')
    tables['product'] = pd.read_parquet(data_dir / 'dim_product.parquet')
    tables['customer'] = pd.read_parquet(data_dir / 'dim_customer.parquet')
    tables['sales'] = pd.read_parquet(data_dir / 'fact_sales.parquet')
    
    return tables

def analyze_sales_by_time(tables):
    """Analyze sales trends over time."""
    sales_time = tables['sales'].merge(tables['time'], on='date_key')
    
    print("SALES ANALYSIS BY TIME")
    print("="*40)
    
    # Sales by year
    yearly_sales = sales_time.groupby('year').agg({
        'gross_sales': 'sum',
        'profit': 'sum',
        'quantity': 'sum'
    }).round(2)
    print("\nSales by Year:")
    print(yearly_sales)
    
    # Sales by quarter
    quarterly_sales = sales_time.groupby(['year', 'quarter']).agg({
        'gross_sales': 'sum',
        'profit': 'sum'
    }).round(2)
    print("\nSales by Quarter (last 8 quarters):")
    print(quarterly_sales.tail(8))
    
    # Weekend vs Weekday sales
    weekend_analysis = sales_time.groupby('is_weekend').agg({
        'gross_sales': ['sum', 'mean'],
        'quantity': ['sum', 'mean']
    }).round(2)
    weekend_analysis.index = ['Weekday', 'Weekend']
    print("\nWeekend vs Weekday Analysis:")
    print(weekend_analysis)

def analyze_sales_by_geography(tables):
    """Analyze sales by geographic regions."""
    sales_geo = tables['sales'].merge(tables['geography'], on='geography_key')
    
    print("\n\nSALES ANALYSIS BY GEOGRAPHY")
    print("="*40)
    
    # Sales by region
    regional_sales = sales_geo.groupby('region').agg({
        'gross_sales': 'sum',
        'profit': 'sum',
        'quantity': 'sum'
    }).round(2).sort_values('gross_sales', ascending=False)
    print("\nSales by Region:")
    print(regional_sales)
    
    # Top 10 countries
    country_sales = sales_geo.groupby('country').agg({
        'gross_sales': 'sum',
        'profit': 'sum'
    }).round(2).sort_values('gross_sales', ascending=False).head(10)
    print("\nTop 10 Countries by Sales:")
    print(country_sales)

def analyze_sales_by_product(tables):
    """Analyze sales by product categories."""
    sales_product = tables['sales'].merge(tables['product'], on='product_key')
    
    print("\n\nSALES ANALYSIS BY PRODUCT")
    print("="*40)
    
    # Sales by category
    category_sales = sales_product.groupby('category').agg({
        'gross_sales': 'sum',
        'profit': 'sum',
        'quantity': 'sum'
    }).round(2).sort_values('gross_sales', ascending=False)
    print("\nSales by Category:")
    print(category_sales)
    
    # Profit margin by category
    category_sales['profit_margin'] = (category_sales['profit'] / category_sales['gross_sales'] * 100).round(2)
    print("\nProfit Margin by Category (%):")
    print(category_sales[['profit_margin']].sort_values('profit_margin', ascending=False))
    
    # Top 10 products
    product_sales = sales_product.groupby('product_name').agg({
        'gross_sales': 'sum',
        'quantity': 'sum'
    }).round(2).sort_values('gross_sales', ascending=False).head(10)
    print("\nTop 10 Products by Sales:")
    print(product_sales)

def analyze_customer_segments(tables):
    """Analyze sales by customer segments."""
    sales_customer = tables['sales'].merge(tables['customer'], on='customer_key')
    
    print("\n\nCUSTOMER SEGMENT ANALYSIS")
    print("="*40)
    
    # Sales by customer type
    customer_sales = sales_customer.groupby('customer_type').agg({
        'gross_sales': ['sum', 'mean'],
        'profit': ['sum', 'mean'],
        'customer_key': 'nunique'
    }).round(2)
    customer_sales.columns = ['Total Sales', 'Avg Sales per Order', 'Total Profit', 'Avg Profit per Order', 'Unique Customers']
    print("\nSales by Customer Type:")
    print(customer_sales)

def multidimensional_analysis(tables):
    """Perform multidimensional analysis (drill-down, roll-up)."""
    # Create a comprehensive view
    comprehensive = (tables['sales']
                    .merge(tables['time'], on='date_key')
                    .merge(tables['geography'], on='geography_key')
                    .merge(tables['product'], on='product_key')
                    .merge(tables['customer'], on='customer_key'))
    
    print("\n\nMULTIDIMENSIONAL ANALYSIS")
    print("="*40)
    
    # Sales by Region and Category
    region_category = comprehensive.groupby(['region', 'category'])['gross_sales'].sum().unstack(fill_value=0).round(2)
    print("\nSales by Region and Product Category:")
    print(region_category)
    
    # Monthly trends for top region
    top_region = comprehensive.groupby('region')['gross_sales'].sum().idxmax()
    monthly_trend = (comprehensive[comprehensive['region'] == top_region]
                    .groupby(['year', 'month'])['gross_sales'].sum().tail(12))
    print(f"\nMonthly Sales Trend for {top_region} (last 12 months):")
    for (year, month), sales in monthly_trend.items():
        print(f"{year}-{month:02d}: ${sales:,.2f}")

def main():
    """Run all analyses on the OLAP data."""
    try:
        print("Loading OLAP data from Parquet files...")
        tables = load_data()
        
        print(f"\nData loaded successfully!")
        print(f"Sales records: {len(tables['sales']):,}")
        print(f"Time periods: {len(tables['time']):,}")
        print(f"Geographies: {len(tables['geography']):,}")
        print(f"Products: {len(tables['product']):,}")
        print(f"Customers: {len(tables['customer']):,}")
        
        # Run various analyses
        analyze_sales_by_time(tables)
        analyze_sales_by_geography(tables)
        analyze_sales_by_product(tables)
        analyze_customer_segments(tables)
        multidimensional_analysis(tables)
        
        print("\n" + "="*50)
        print("Analysis complete!")
        
    except FileNotFoundError:
        print("Error: OLAP data files not found. Please run generate_olap_data.py first.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
