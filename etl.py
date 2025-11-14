import requests
import pandas as pd
import sqlite3
from datetime import datetime
import sys

# Configuration
API_URL = "http://127.0.0.1:8000/transactions"
DB_PATH = "mobile_shop_dw.db"

def extract_data():
    """
    Extract data from the FastAPI endpoint
    
    Returns:
    - DataFrame with transaction data
    """
    print("\n" + "=" * 60)
    print("STEP 1: EXTRACTING DATA FROM API")
    print("=" * 60)
    
    try:
        print(f"→ Fetching data from {API_URL}...")
        response = requests.get(API_URL)
        response.raise_for_status()
        
        json_data = response.json()
        df = pd.DataFrame(json_data['data'])
        
        print(f"✓ Successfully extracted {len(df)} records")
        return df
    
    except requests.exceptions.ConnectionError:
        print("✗ Error: Cannot connect to API. Make sure the API server is running.")
        print("  Run: python api.py")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error extracting data: {e}")
        sys.exit(1)

def transform_data(df):
    """
    Transform the extracted data
    
    Parameters:
    - df: Raw DataFrame from API
    
    Returns:
    - Transformed DataFrame
    """
    print("\n" + "=" * 60)
    print("STEP 2: TRANSFORMING DATA")
    print("=" * 60)
    
    # Create a copy to avoid modifying the original
    df_transformed = df.copy()
    
    # Convert datetime
    print("→ Converting datetime columns...")
    df_transformed['Transaction_DateTime'] = pd.to_datetime(df_transformed['Transaction_DateTime'])
    
    # Create age groups
    print("→ Creating age groups...")
    df_transformed['Age_Group'] = pd.cut(
        df_transformed['Customer_Age'],
        bins=[0, 25, 35, 45, 55, 100],
        labels=['18-25', '26-35', '36-45', '46-55', '56+']
    )
    
    # Create month column for time series analysis
    df_transformed['Transaction_Month'] = df_transformed['Transaction_DateTime'].dt.to_period('M').astype(str)
    
    print(f"✓ Transformation complete")
    print(f"  - Added Age_Group column")
    print(f"  - Added Transaction_Month column")
    
    return df_transformed

def create_summary_tables(df):
    """
    Create summary tables for the dashboard
    
    Parameters:
    - df: Transformed DataFrame
    
    Returns:
    - Dictionary of summary DataFrames
    """
    print("\n" + "=" * 60)
    print("STEP 3: CREATING SUMMARY TABLES")
    print("=" * 60)
    
    summaries = {}
    
    # 1. KPI Summary by Dimension (Category, Brand, Model)
    print("→ Creating KPI summary by dimension...")
    kpi_dimensions = []
    
    for dimension in ['Category', 'Brand', 'Model']:
        dim_summary = df.groupby(dimension)['Total_Amount'].sum().reset_index()
        dim_summary.columns = ['dimension_value', 'total_amount']
        dim_summary['dimension'] = dimension
        kpi_dimensions.append(dim_summary)
    
    summaries['kpi_summary_by_dimension'] = pd.concat(kpi_dimensions, ignore_index=True)
    print(f"  ✓ Created KPI summary: {len(summaries['kpi_summary_by_dimension'])} records")
    
    # 2. KPI Order Status Summary
    print("→ Creating KPI order status summary...")
    summaries['kpi_order_status_summary'] = df.groupby(
        ['Order_Type', 'Transaction_Status']
    ).size().reset_index(name='record_count')
    summaries['kpi_order_status_summary'].columns = ['order_type', 'transaction_status', 'record_count']
    print(f"  ✓ Created order status summary: {len(summaries['kpi_order_status_summary'])} records")
    
    # 3. Customer Analysis Summary
    print("→ Creating customer analysis summary...")
    customer_summary = df.groupby(['Age_Group', 'Customer_Gender', 'Transaction_Month']).agg({
        'Discount_Applied': 'mean',
        'Customer_Rating': 'mean',
        'Transaction_ID': 'count'
    }).reset_index()
    
    customer_summary.columns = [
        'age_group', 'gender', 'transaction_month',
        'avg_discount_applied', 'avg_customer_rating', 'transaction_count'
    ]
    summaries['customer_analysis_summary'] = customer_summary
    print(f"  ✓ Created customer summary: {len(summaries['customer_analysis_summary'])} records")
    
    # 4. Product Analysis Summary
    print("→ Creating product analysis summary...")
    product_dimensions = []
    
    for dimension in ['Category', 'Brand', 'Model', 'Product_Name']:
        dim_summary = df.groupby([dimension, 'Product_Type']).size().reset_index(name='record_count')
        dim_summary.columns = ['dimension_value', 'product_type', 'record_count']
        dim_summary['dimension'] = dimension
        product_dimensions.append(dim_summary)
    
    summaries['product_analysis_summary'] = pd.concat(product_dimensions, ignore_index=True)
    print(f"  ✓ Created product summary: {len(summaries['product_analysis_summary'])} records")
    
    return summaries

def load_data(df, summaries):
    """
    Load data into the SQLite data warehouse
    
    Parameters:
    - df: Transformed raw data DataFrame
    - summaries: Dictionary of summary DataFrames
    """
    print("\n" + "=" * 60)
    print("STEP 4: LOADING DATA INTO DATA WAREHOUSE")
    print("=" * 60)
    
    try:
        # Connect to SQLite database
        print(f"→ Connecting to database: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        
        # Load raw transactions
        print("→ Loading raw transactions table...")
        df.to_sql('raw_transactions', conn, if_exists='replace', index=False)
        print(f"  ✓ Loaded {len(df)} records into raw_transactions")
        
        # Load summary tables
        for table_name, summary_df in summaries.items():
            print(f"→ Loading {table_name}...")
            summary_df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"  ✓ Loaded {len(summary_df)} records into {table_name}")
        
        # Create indexes for better query performance
        print("→ Creating indexes...")
        cursor = conn.cursor()
        
        # Indexes on raw_transactions
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_category ON raw_transactions(Category)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_brand ON raw_transactions(Brand)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_type ON raw_transactions(Order_Type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_age_group ON raw_transactions(Age_Group)")
        
        conn.commit()
        print("  ✓ Indexes created")
        
        # Display database statistics
        print("\n" + "-" * 60)
        print("DATABASE STATISTICS")
        print("-" * 60)
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  {table_name}: {count:,} records")
        
        conn.close()
        print("\n✓ Data warehouse update complete!")
        
    except Exception as e:
        print(f"✗ Error loading data: {e}")
        sys.exit(1)

def main():
    """Main ETL pipeline execution"""
    print("\n" + "=" * 60)
    print("MOBILE SHOP ETL PIPELINE")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Execute ETL steps
    df_raw = extract_data()
    df_transformed = transform_data(df_raw)
    summaries = create_summary_tables(df_transformed)
    load_data(df_transformed, summaries)
    
    print("\n" + "=" * 60)
    print("ETL PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data warehouse: {DB_PATH}")
    print("\nYou can now run the dashboard:")
    print("  python dashboard.py")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()

