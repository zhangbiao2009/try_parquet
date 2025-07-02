#!/usr/bin/env python3
"""
Generate sample OLAP data and write to Parquet and CSV files.
This creates a star schema with fact and dimension tables suitable for OLAP analysis.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path

# Set random seed for reproducible data
np.random.seed(42)
random.seed(42)

def generate_time_dimension(start_date='2020-01-01', end_date='2024-12-31'):
    """Generate time dimension with various time hierarchies."""
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    time_dim = pd.DataFrame({
        'date_key': range(len(date_range)),
        'date': date_range,
        'year': date_range.year,
        'quarter': date_range.quarter,
        'month': date_range.month,
        'month_name': date_range.strftime('%B'),
        'day': date_range.day,
        'day_of_week': date_range.dayofweek + 1,
        'day_name': date_range.strftime('%A'),
        'week_of_year': date_range.isocalendar().week,
        'is_weekend': (date_range.dayofweek >= 5).astype(int),
        'fiscal_year': np.where(date_range.month >= 4, date_range.year + 1, date_range.year)
    })
    
    return time_dim

def generate_geography_dimension():
    """Generate geography dimension with hierarchical structure."""
    regions = ['North America', 'Europe', 'Asia Pacific', 'Latin America']
    countries = {
        'North America': ['USA', 'Canada', 'Mexico'],
        'Europe': ['Germany', 'France', 'UK', 'Italy', 'Spain'],
        'Asia Pacific': ['China', 'Japan', 'Australia', 'India', 'South Korea'],
        'Latin America': ['Brazil', 'Argentina', 'Chile', 'Colombia']
    }
    
    cities = {
        'USA': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'],
        'Canada': ['Toronto', 'Vancouver', 'Montreal', 'Calgary'],
        'Mexico': ['Mexico City', 'Guadalajara', 'Monterrey'],
        'Germany': ['Berlin', 'Munich', 'Hamburg', 'Frankfurt'],
        'France': ['Paris', 'Lyon', 'Marseille', 'Toulouse'],
        'UK': ['London', 'Manchester', 'Birmingham', 'Glasgow'],
        'Italy': ['Rome', 'Milan', 'Naples', 'Turin'],
        'Spain': ['Madrid', 'Barcelona', 'Valencia', 'Seville'],
        'China': ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen'],
        'Japan': ['Tokyo', 'Osaka', 'Nagoya', 'Fukuoka'],
        'Australia': ['Sydney', 'Melbourne', 'Brisbane', 'Perth'],
        'India': ['Mumbai', 'Delhi', 'Bangalore', 'Chennai'],
        'South Korea': ['Seoul', 'Busan', 'Incheon'],
        'Brazil': ['São Paulo', 'Rio de Janeiro', 'Brasília'],
        'Argentina': ['Buenos Aires', 'Córdoba', 'Rosario'],
        'Chile': ['Santiago', 'Valparaíso', 'Concepción'],
        'Colombia': ['Bogotá', 'Medellín', 'Cali']
    }
    
    geo_data = []
    geo_key = 1
    
    for region in regions:
        for country in countries[region]:
            for city in cities[country]:
                geo_data.append({
                    'geography_key': geo_key,
                    'city': city,
                    'country': country,
                    'region': region
                })
                geo_key += 1
    
    return pd.DataFrame(geo_data)

def generate_product_dimension():
    """Generate product dimension with hierarchical categories."""
    categories = {
        'Electronics': {
            'Computers': ['Laptop', 'Desktop', 'Tablet', 'Monitor'],
            'Mobile': ['Smartphone', 'Feature Phone', 'Accessories'],
            'Audio': ['Headphones', 'Speakers', 'Microphone']
        },
        'Clothing': {
            'Men': ['Shirts', 'Pants', 'Shoes', 'Accessories'],
            'Women': ['Dresses', 'Tops', 'Shoes', 'Accessories'],
            'Kids': ['Clothing', 'Shoes', 'Toys']
        },
        'Home & Garden': {
            'Furniture': ['Chairs', 'Tables', 'Sofas', 'Storage'],
            'Kitchen': ['Appliances', 'Cookware', 'Utensils'],
            'Garden': ['Tools', 'Plants', 'Outdoor Furniture']
        }
    }
    
    product_data = []
    product_key = 1
    
    for category, subcategories in categories.items():
        for subcategory, products in subcategories.items():
            for product in products:
                # Generate multiple SKUs per product type
                for i in range(random.randint(3, 8)):
                    product_data.append({
                        'product_key': product_key,
                        'sku': f'{product[:3].upper()}{product_key:04d}',
                        'product_name': f'{product} Model {chr(65+i)}',
                        'product_type': product,
                        'subcategory': subcategory,
                        'category': category,
                        'unit_cost': round(random.uniform(10, 500), 2),
                        'unit_price': round(random.uniform(15, 750), 2)
                    })
                    product_key += 1
    
    return pd.DataFrame(product_data)

def generate_customer_dimension():
    """Generate customer dimension."""
    customer_types = ['Individual', 'Small Business', 'Enterprise']
    
    customer_data = []
    for customer_key in range(1, 1001):  # 1000 customers
        customer_data.append({
            'customer_key': customer_key,
            'customer_id': f'CUST{customer_key:06d}',
            'customer_type': random.choice(customer_types),
            'registration_date': datetime(2019, 1, 1) + timedelta(days=random.randint(0, 1825))
        })
    
    return pd.DataFrame(customer_data)

def generate_sales_fact(time_dim, geo_dim, product_dim, customer_dim, num_records=50000):
    """Generate sales fact table with realistic patterns using vectorized operations."""
    print(f"Generating {num_records} sales records using vectorized approach...")
    
    # Generate all random selections at once (much faster)
    np.random.seed(42)  # Ensure reproducibility
    date_keys = np.random.choice(time_dim['date_key'].values, num_records)
    geo_keys = np.random.choice(geo_dim['geography_key'].values, num_records)
    product_keys = np.random.choice(product_dim['product_key'].values, num_records)
    customer_keys = np.random.choice(customer_dim['customer_key'].values, num_records)
    
    # Generate base quantities
    base_quantities = np.random.randint(1, 11, num_records)
    
    # Create base DataFrame
    sales_fact = pd.DataFrame({
        'sales_key': range(1, num_records + 1),
        'date_key': date_keys,
        'geography_key': geo_keys,
        'product_key': product_keys,
        'customer_key': customer_keys,
        'base_quantity': base_quantities
    })
    
    print("Joining with dimension tables...")
    
    # Join with product dimension to get pricing (much faster than loops)
    sales_fact = sales_fact.merge(
        product_dim[['product_key', 'unit_price', 'unit_cost']], 
        on='product_key'
    )
    
    # Join with time dimension to get weekend info
    sales_fact = sales_fact.merge(
        time_dim[['date_key', 'is_weekend']], 
        on='date_key'
    )
    
    print("Calculating derived fields...")
    
    # Apply weekend boost to quantities (vectorized)
    weekend_multiplier = np.where(
        sales_fact['is_weekend'] == 1,
        np.random.uniform(1.2, 1.8, num_records),
        1.0
    )
    sales_fact['quantity'] = np.maximum(1, (sales_fact['base_quantity'] * weekend_multiplier).astype(int))
    
    # Generate price variations (discounts, promotions)
    price_modifiers = np.random.uniform(0.8, 1.1, num_records)
    sales_fact['unit_price'] = np.round(sales_fact['unit_price'] * price_modifiers, 2)
    
    # Calculate financial metrics (vectorized)
    sales_fact['gross_sales'] = np.round(sales_fact['quantity'] * sales_fact['unit_price'], 2)
    sales_fact['total_cost'] = np.round(sales_fact['quantity'] * sales_fact['unit_cost'], 2)
    sales_fact['profit'] = np.round(sales_fact['gross_sales'] - sales_fact['total_cost'], 2)
    
    # Clean up temporary columns
    sales_fact = sales_fact.drop(['base_quantity', 'is_weekend'], axis=1)
    
    # Reorder columns to match original format
    sales_fact = sales_fact[['sales_key', 'date_key', 'geography_key', 'product_key', 
                           'customer_key', 'quantity', 'unit_price', 'unit_cost', 
                           'gross_sales', 'total_cost', 'profit']]
    
    print(f"Generated {len(sales_fact):,} sales records successfully!")
    return sales_fact

def main():
    """Generate all dimensions and fact table, then save to both Parquet and CSV files."""
    # Create output directories
    parquet_dir = Path('olap_data')
    csv_dir = Path('csv_data')
    parquet_dir.mkdir(exist_ok=True)
    csv_dir.mkdir(exist_ok=True)
    
    print("Generating OLAP sample data...")
    
    # Generate dimension tables
    print("Generating time dimension...")
    time_dim = generate_time_dimension()
    
    print("Generating geography dimension...")
    geo_dim = generate_geography_dimension()
    
    print("Generating product dimension...")
    product_dim = generate_product_dimension()
    
    print("Generating customer dimension...")
    customer_dim = generate_customer_dimension()
    
    # Generate fact table
    print("Generating sales fact table...")
    sales_fact = generate_sales_fact(time_dim, geo_dim, product_dim, customer_dim, 1000000)
    
    # Save to Parquet files
    print("Saving to Parquet files...")
    time_dim.to_parquet(parquet_dir / 'dim_time.parquet', index=False)
    geo_dim.to_parquet(parquet_dir / 'dim_geography.parquet', index=False)
    product_dim.to_parquet(parquet_dir / 'dim_product.parquet', index=False)
    customer_dim.to_parquet(parquet_dir / 'dim_customer.parquet', index=False)
    sales_fact.to_parquet(parquet_dir / 'fact_sales.parquet', index=False)
    
    # Save to CSV files
    print("Saving to CSV files...")
    time_dim.to_csv(csv_dir / 'dim_time.csv', index=False)
    geo_dim.to_csv(csv_dir / 'dim_geography.csv', index=False)
    product_dim.to_csv(csv_dir / 'dim_product.csv', index=False)
    customer_dim.to_csv(csv_dir / 'dim_customer.csv', index=False)
    sales_fact.to_csv(csv_dir / 'fact_sales.csv', index=False)
    
    # Print summary statistics
    print("\n" + "="*50)
    print("OLAP Data Generation Complete!")
    print("="*50)
    print(f"Time dimension: {len(time_dim):,} records")
    print(f"Geography dimension: {len(geo_dim):,} records")
    print(f"Product dimension: {len(product_dim):,} records")
    print(f"Customer dimension: {len(customer_dim):,} records")
    print(f"Sales fact table: {len(sales_fact):,} records")
    print(f"\nParquet files saved to: {parquet_dir.absolute()}")
    print(f"CSV files saved to: {csv_dir.absolute()}")
    
    # Show sample data
    print("\nSample Sales Data (first 5 records):")
    print(sales_fact.head())
    
    print("\nTotal Sales by Year:")
    sales_by_year = sales_fact.merge(time_dim, on='date_key').groupby('year')['gross_sales'].sum()
    print(sales_by_year.apply(lambda x: f"${x:,.2f}"))

if __name__ == "__main__":
    main()
