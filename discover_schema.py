#!/usr/bin/env python3
"""
Demonstrate how to discover Parquet file schemas before processing.
This shows various ways to inspect unknown Parquet files.
"""

import duckdb
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

def discover_schema_pyarrow():
    """Use PyArrow to discover Parquet schema."""
    print("SCHEMA DISCOVERY USING PYARROW")
    print("="*40)
    
    data_dir = Path('olap_data')
    
    for parquet_file in data_dir.glob('*.parquet'):
        print(f"\n📁 File: {parquet_file.name}")
        
        # Read schema without loading data
        parquet_file_obj = pq.ParquetFile(parquet_file)
        schema = parquet_file_obj.schema
        
        print(f"   Columns: {len(schema)}")
        print(f"   Rows: {parquet_file_obj.metadata.num_rows:,}")
        
        # Show column details
        for i, column in enumerate(schema.names):
            column_type = schema.column(i).logical_type
            print(f"   {i+1:2d}. {column:20s} ({column_type})")

def discover_schema_duckdb():
    """Use DuckDB to discover Parquet schema."""
    print("\n\nSCHEMA DISCOVERY USING DUCKDB")
    print("="*40)
    
    conn = duckdb.connect()
    data_dir = Path('olap_data')
    
    for parquet_file in data_dir.glob('*.parquet'):
        print(f"\n📁 File: {parquet_file.name}")
        
        # Use DESCRIBE to get schema
        schema_info = conn.execute(f"""
            DESCRIBE SELECT * FROM '{parquet_file}' LIMIT 0
        """).fetchdf()
        
        print(f"   Columns: {len(schema_info)}")
        print("   Schema:")
        for _, row in schema_info.iterrows():
            print(f"      {row['column_name']:20s} {row['column_type']}")

def discover_schema_pandas():
    """Use pandas to discover Parquet schema."""
    print("\n\nSCHEMA DISCOVERY USING PANDAS")
    print("="*40)
    
    data_dir = Path('olap_data')
    
    for parquet_file in data_dir.glob('*.parquet'):
        print(f"\n📁 File: {parquet_file.name}")
        
        # Read just the first few rows to get schema
        sample_df = pd.read_parquet(parquet_file)
        sample_df = sample_df.head(1)  # Take only first row
        
        print(f"   Columns: {len(sample_df.columns)}")
        print(f"   Shape: {sample_df.shape}")
        print("   Schema:")
        for col in sample_df.columns:
            dtype = sample_df[col].dtype
            print(f"      {col:20s} {dtype}")

def discover_data_preview():
    """Preview actual data to understand content."""
    print("\n\nDATA PREVIEW")
    print("="*40)
    
    conn = duckdb.connect()
    data_dir = Path('olap_data')
    
    for parquet_file in data_dir.glob('*.parquet'):
        print(f"\n📁 File: {parquet_file.name}")
        
        # Get first 3 rows
        preview = conn.execute(f"""
            SELECT * FROM '{parquet_file}' LIMIT 3
        """).fetchdf()
        
        print("   Sample data:")
        print(preview.to_string(index=False))

def auto_generate_analysis():
    """Automatically generate analysis queries based on discovered schema."""
    print("\n\nAUTO-GENERATED ANALYSIS")
    print("="*40)
    
    conn = duckdb.connect()
    
    # Discover fact table schema
    fact_schema = conn.execute("""
        DESCRIBE SELECT * FROM 'olap_data/fact_sales.parquet' LIMIT 0
    """).fetchdf()
    
    print("🔍 Discovered fact table schema:")
    for _, row in fact_schema.iterrows():
        print(f"   {row['column_name']:20s} {row['column_type']}")
    
    # Find numeric columns for aggregation
    numeric_columns = fact_schema[
        fact_schema['column_type'].str.contains('DOUBLE|INTEGER|DECIMAL', na=False)
    ]['column_name'].tolist()
    
    # Find key columns (likely foreign keys)
    key_columns = fact_schema[
        fact_schema['column_name'].str.contains('_key', na=False)
    ]['column_name'].tolist()
    
    print(f"\n📊 Found numeric columns for aggregation: {numeric_columns}")
    print(f"🔑 Found key columns for joins: {key_columns}")
    
    # Auto-generate a simple aggregation query
    if numeric_columns:
        agg_columns = ', '.join([f"SUM({col}) as total_{col}" for col in numeric_columns[:3]])
        
        query = f"""
        SELECT 
            COUNT(*) as record_count,
            {agg_columns}
        FROM 'olap_data/fact_sales.parquet'
        """
        
        print(f"\n🤖 Auto-generated summary query:")
        print(query)
        
        result = conn.execute(query).fetchdf()
        print("\n📈 Results:")
        print(result.to_string(index=False))

def discover_relationships():
    """Try to discover relationships between tables."""
    print("\n\nRELATIONSHIP DISCOVERY")
    print("="*40)
    
    conn = duckdb.connect()
    
    # Get all tables and their key columns
    tables = {}
    data_dir = Path('olap_data')
    
    for parquet_file in data_dir.glob('*.parquet'):
        table_name = parquet_file.stem
        
        schema = conn.execute(f"""
            DESCRIBE SELECT * FROM '{parquet_file}' LIMIT 0
        """).fetchdf()
        
        # Find key columns
        keys = schema[schema['column_name'].str.contains('_key', na=False)]['column_name'].tolist()
        tables[table_name] = keys
    
    print("🔗 Discovered potential relationships:")
    for table, keys in tables.items():
        print(f"   {table:20s} -> {keys}")
    
    # Look for common keys (potential joins)
    all_keys = []
    for keys in tables.values():
        all_keys.extend(keys)
    
    common_keys = set([k for k in all_keys if all_keys.count(k) > 1])
    
    if common_keys:
        print(f"\n🎯 Common keys for joins: {list(common_keys)}")
        
        # Try a sample join
        if 'date_key' in common_keys:
            sample_join = conn.execute("""
                SELECT COUNT(*) as join_count
                FROM 'olap_data/fact_sales.parquet' s
                JOIN 'olap_data/dim_time.parquet' t ON s.date_key = t.date_key
            """).fetchone()[0]
            
            print(f"✅ Sample join on date_key successful: {sample_join:,} records")

def main():
    """Demonstrate various schema discovery techniques."""
    print("PARQUET SCHEMA DISCOVERY TECHNIQUES")
    print("="*50)
    
    try:
        discover_schema_pyarrow()
        discover_schema_duckdb() 
        discover_schema_pandas()
        discover_data_preview()
        auto_generate_analysis()
        discover_relationships()
        
        print("\n" + "="*50)
        print("Schema discovery complete!")
        print("\nIn practice, you would:")
        print("1. Use these techniques to understand unknown data")
        print("2. Generate analysis queries dynamically")
        print("3. Build flexible, schema-agnostic processing")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
