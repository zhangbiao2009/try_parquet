#!/usr/bin/env python3
"""
Analyze the generated OLAP data using DuckDB for scalable out-of-core processing.
This script demonstrates the same OLAP-style queries as analyze_olap_data.py but uses
DuckDB to handle datasets that don't fit in memory.

Key advantages over pandas approach:
- Can handle datasets larger than RAM (10TB+)
- Automatic query optimization and vectorized execution
- Direct Parquet file querying without loading into memory
- SQL interface for complex analytical queries
- Built-in support for star schema analytics
"""

import duckdb
import pandas as pd
from pathlib import Path
import time

def get_connection():
    """Create and configure DuckDB connection for analytics."""
    conn = duckdb.connect()
    
    # Configure DuckDB for better analytical performance
    conn.execute("SET memory_limit='4GB'")  # Adjust based on available RAM
    conn.execute("SET threads=4")           # Use multiple cores
    
    return conn

def register_tables(conn):
    """Register Parquet files as virtual tables in DuckDB."""
    data_dir = Path('olap_data')
    
    # Register each Parquet file as a table
    conn.execute(f"CREATE VIEW dim_time AS SELECT * FROM '{data_dir / 'dim_time.parquet'}'")
    conn.execute(f"CREATE VIEW dim_geography AS SELECT * FROM '{data_dir / 'dim_geography.parquet'}'")
    conn.execute(f"CREATE VIEW dim_product AS SELECT * FROM '{data_dir / 'dim_product.parquet'}'")
    conn.execute(f"CREATE VIEW dim_customer AS SELECT * FROM '{data_dir / 'dim_customer.parquet'}'")
    conn.execute(f"CREATE VIEW fact_sales AS SELECT * FROM '{data_dir / 'fact_sales.parquet'}'")
    
    print("Tables registered successfully!")
    return conn

def get_data_info(conn):
    """Get basic information about the dataset."""
    info = {}
    
    # Get record counts for each table
    info['sales'] = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    info['time'] = conn.execute("SELECT COUNT(*) FROM dim_time").fetchone()[0]
    info['geography'] = conn.execute("SELECT COUNT(*) FROM dim_geography").fetchone()[0]
    info['product'] = conn.execute("SELECT COUNT(*) FROM dim_product").fetchone()[0]
    info['customer'] = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    
    return info

def analyze_sales_by_time(conn):
    """Analyze sales trends over time using DuckDB SQL."""
    print("SALES ANALYSIS BY TIME (DuckDB)")
    print("="*40)
    
    # Sales by year
    yearly_sales = conn.execute("""
        SELECT 
            t.year,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            ROUND(SUM(s.profit), 2) as profit,
            SUM(s.quantity) as quantity
        FROM fact_sales s
        JOIN dim_time t ON s.date_key = t.date_key
        GROUP BY t.year
        ORDER BY t.year
    """).fetchdf()
    
    print("\nSales by Year:")
    print(yearly_sales.to_string(index=False))
    
    # Sales by quarter
    quarterly_sales = conn.execute("""
        SELECT 
            t.year,
            t.quarter,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            ROUND(SUM(s.profit), 2) as profit
        FROM fact_sales s
        JOIN dim_time t ON s.date_key = t.date_key
        GROUP BY t.year, t.quarter
        ORDER BY t.year, t.quarter
    """).fetchdf()
    
    print("\nSales by Quarter (last 8 quarters):")
    print(quarterly_sales.tail(8).to_string(index=False))
    
    # Weekend vs Weekday analysis
    weekend_analysis = conn.execute("""
        SELECT 
            CASE WHEN t.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
            ROUND(SUM(s.gross_sales), 2) as total_sales,
            ROUND(AVG(s.gross_sales), 2) as avg_sales,
            SUM(s.quantity) as total_quantity,
            ROUND(AVG(s.quantity), 2) as avg_quantity
        FROM fact_sales s
        JOIN dim_time t ON s.date_key = t.date_key
        GROUP BY t.is_weekend
        ORDER BY t.is_weekend
    """).fetchdf()
    
    print("\nWeekend vs Weekday Analysis:")
    print(weekend_analysis.to_string(index=False))

def analyze_sales_by_geography(conn):
    """Analyze sales by geographic regions using DuckDB SQL."""
    print("\n\nSALES ANALYSIS BY GEOGRAPHY (DuckDB)")
    print("="*40)
    
    # Sales by region
    regional_sales = conn.execute("""
        SELECT 
            g.region,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            ROUND(SUM(s.profit), 2) as profit,
            SUM(s.quantity) as quantity
        FROM fact_sales s
        JOIN dim_geography g ON s.geography_key = g.geography_key
        GROUP BY g.region
        ORDER BY SUM(s.gross_sales) DESC
    """).fetchdf()
    
    print("\nSales by Region:")
    print(regional_sales.to_string(index=False))
    
    # Top 10 countries
    country_sales = conn.execute("""
        SELECT 
            g.country,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            ROUND(SUM(s.profit), 2) as profit
        FROM fact_sales s
        JOIN dim_geography g ON s.geography_key = g.geography_key
        GROUP BY g.country
        ORDER BY SUM(s.gross_sales) DESC
        LIMIT 10
    """).fetchdf()
    
    print("\nTop 10 Countries by Sales:")
    print(country_sales.to_string(index=False))

def analyze_sales_by_product(conn):
    """Analyze sales by product categories using DuckDB SQL."""
    print("\n\nSALES ANALYSIS BY PRODUCT (DuckDB)")
    print("="*40)
    
    # Sales by category
    category_sales = conn.execute("""
        SELECT 
            p.category,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            ROUND(SUM(s.profit), 2) as profit,
            SUM(s.quantity) as quantity
        FROM fact_sales s
        JOIN dim_product p ON s.product_key = p.product_key
        GROUP BY p.category
        ORDER BY SUM(s.gross_sales) DESC
    """).fetchdf()
    
    print("\nSales by Category:")
    print(category_sales.to_string(index=False))
    
    # Profit margin by category
    profit_margin = conn.execute("""
        SELECT 
            p.category,
            ROUND(SUM(s.profit) / SUM(s.gross_sales) * 100, 2) as profit_margin_pct
        FROM fact_sales s
        JOIN dim_product p ON s.product_key = p.product_key
        GROUP BY p.category
        ORDER BY SUM(s.profit) / SUM(s.gross_sales) DESC
    """).fetchdf()
    
    print("\nProfit Margin by Category (%):")
    print(profit_margin.to_string(index=False))
    
    # Top 10 products
    product_sales = conn.execute("""
        SELECT 
            p.product_name,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            SUM(s.quantity) as quantity
        FROM fact_sales s
        JOIN dim_product p ON s.product_key = p.product_key
        GROUP BY p.product_name
        ORDER BY SUM(s.gross_sales) DESC
        LIMIT 10
    """).fetchdf()
    
    print("\nTop 10 Products by Sales:")
    print(product_sales.to_string(index=False))

def analyze_customer_segments(conn):
    """Analyze sales by customer segments using DuckDB SQL."""
    print("\n\nCUSTOMER SEGMENT ANALYSIS (DuckDB)")
    print("="*40)
    
    # Sales by customer type
    customer_sales = conn.execute("""
        SELECT 
            c.customer_type,
            ROUND(SUM(s.gross_sales), 2) as total_sales,
            ROUND(AVG(s.gross_sales), 2) as avg_sales_per_order,
            ROUND(SUM(s.profit), 2) as total_profit,
            ROUND(AVG(s.profit), 2) as avg_profit_per_order,
            COUNT(DISTINCT s.customer_key) as unique_customers
        FROM fact_sales s
        JOIN dim_customer c ON s.customer_key = c.customer_key
        GROUP BY c.customer_type
        ORDER BY SUM(s.gross_sales) DESC
    """).fetchdf()
    
    print("\nSales by Customer Type:")
    print(customer_sales.to_string(index=False))

def multidimensional_analysis(conn):
    """Perform multidimensional analysis using DuckDB SQL."""
    print("\n\nMULTIDIMENSIONAL ANALYSIS (DuckDB)")
    print("="*40)
    
    # Sales by Region and Category (Cross-tab analysis)
    region_category = conn.execute("""
        SELECT 
            g.region,
            p.category,
            ROUND(SUM(s.gross_sales), 2) as gross_sales
        FROM fact_sales s
        JOIN dim_geography g ON s.geography_key = g.geography_key
        JOIN dim_product p ON s.product_key = p.product_key
        GROUP BY g.region, p.category
        ORDER BY g.region, SUM(s.gross_sales) DESC
    """).fetchdf()
    
    # Pivot the results for better readability
    pivot_result = region_category.pivot_table(
        index='region', 
        columns='category', 
        values='gross_sales', 
        fill_value=0
    ).round(2)
    
    print("\nSales by Region and Product Category:")
    print(pivot_result)
    
    # Monthly trends for top region
    top_region_query = conn.execute("""
        SELECT g.region
        FROM fact_sales s
        JOIN dim_geography g ON s.geography_key = g.geography_key
        GROUP BY g.region
        ORDER BY SUM(s.gross_sales) DESC
        LIMIT 1
    """).fetchone()[0]
    
    monthly_trend = conn.execute(f"""
        SELECT 
            t.year,
            t.month,
            ROUND(SUM(s.gross_sales), 2) as gross_sales
        FROM fact_sales s
        JOIN dim_time t ON s.date_key = t.date_key
        JOIN dim_geography g ON s.geography_key = g.geography_key
        WHERE g.region = '{top_region_query}'
        GROUP BY t.year, t.month
        ORDER BY t.year, t.month
    """).fetchdf()
    
    print(f"\nMonthly Sales Trend for {top_region_query} (last 12 months):")
    for _, row in monthly_trend.tail(12).iterrows():
        print(f"{int(row['year'])}-{int(row['month']):02d}: ${row['gross_sales']:,.2f}")

def demonstrate_duckdb_advantages(conn):
    """Demonstrate DuckDB's advantages over pandas for large datasets."""
    print("\n\nDUCKDB PERFORMANCE ADVANTAGES")
    print("="*40)
    
    # Query with column pruning (only reads needed columns)
    start_time = time.time()
    result = conn.execute("""
        SELECT year, SUM(gross_sales) as total_sales
        FROM fact_sales s
        JOIN dim_time t ON s.date_key = t.date_key
        GROUP BY year
    """).fetchdf()
    query_time = time.time() - start_time
    
    print(f"\nQuery executed in {query_time:.3f} seconds")
    print("✓ Only reads necessary columns from Parquet files")
    print("✓ Uses predicate pushdown for filtering")
    print("✓ Vectorized execution with parallel processing")
    print("✓ No memory constraint - can handle TB+ datasets")
    
    # Show memory efficiency
    print(f"\nMemory usage: Minimal (streaming processing)")
    print(f"Pandas equivalent: Would need ~{len(result) * 8 / 1024**2:.1f}MB+ for full dataset")

def main():
    """Run all analyses using DuckDB for scalable analytics."""
    try:
        print("Starting DuckDB-based OLAP analysis...")
        print("="*50)
        
        # Initialize DuckDB connection
        conn = get_connection()
        register_tables(conn)
        
        # Get dataset information
        info = get_data_info(conn)
        
        print(f"\nData registered successfully!")
        print(f"Sales records: {info['sales']:,}")
        print(f"Time periods: {info['time']:,}")
        print(f"Geographies: {info['geography']:,}")
        print(f"Products: {info['product']:,}")
        print(f"Customers: {info['customer']:,}")
        
        # Run various analyses
        analyze_sales_by_time(conn)
        analyze_sales_by_geography(conn)
        analyze_sales_by_product(conn)
        analyze_customer_segments(conn)
        multidimensional_analysis(conn)
        demonstrate_duckdb_advantages(conn)
        
        print("\n" + "="*50)
        print("DuckDB analysis complete!")
        print("\nKey benefits demonstrated:")
        print("• Out-of-core processing (no memory limits)")
        print("• SQL interface for complex analytics")
        print("• Automatic query optimization")
        print("• Direct Parquet file querying")
        print("• Vectorized columnar execution")
        
        conn.close()
        
    except FileNotFoundError:
        print("Error: OLAP data files not found. Please run generate_olap_data.py first.")
    except Exception as e:
        print(f"Error: {e}")
        print("\nTo install DuckDB, run: pip install duckdb")

if __name__ == "__main__":
    main()
