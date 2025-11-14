import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import sys

# Configuration
API_URL = "http://127.0.0.1:8000/transactions"
DB_PATH = "mobile_shop_dw.db"

def extract_data():
    """Extract data from the FastAPI endpoint"""
    print("\n" + "=" * 70)
    print("STEP 1: EXTRACTING DATA FROM API")
    print("=" * 70)
    
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
    """Transform the extracted data"""
    print("\n" + "=" * 70)
    print("STEP 2: TRANSFORMING DATA")
    print("=" * 70)
    
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
    
    # Extract date components
    df_transformed['Date'] = df_transformed['Transaction_DateTime'].dt.date
    df_transformed['Year'] = df_transformed['Transaction_DateTime'].dt.year
    df_transformed['Month'] = df_transformed['Transaction_DateTime'].dt.month
    df_transformed['Day'] = df_transformed['Transaction_DateTime'].dt.day
    df_transformed['Year_Month'] = df_transformed['Transaction_DateTime'].dt.to_period('M').astype(str)
    
    # add a null df
    df_transformed["Product_Key"]  = range(1, len(df_transformed) + 1)
    df_transformed["Customer_Key"] = range(1, len(df_transformed) + 1)
    df_transformed["Staff_Key"]    = range(1, len(df_transformed) + 1)
    df_transformed["Supplier_Key"] = range(1, len(df_transformed) + 1)

    print(f"✓ Transformation complete")
    return df_transformed

def populate_dim_date(conn, df):
    """Populate the date dimension table"""
    print("→ Populating dim_date...")
    
    # Get min and max dates
    min_date = df['Transaction_DateTime'].min().date()
    max_date = df['Transaction_DateTime'].max().date()
    
    # Generate all dates in range
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    
    dim_date_data = []
    for date in date_range:
        date_key = int(date.strftime('%Y%m%d'))
        dim_date_data.append({
            'date_key': date_key,
            'full_date': date.date(),
            'year': date.year,
            'quarter': (date.month - 1) // 3 + 1,
            'month': date.month,
            'month_name': date.strftime('%B'),
            'week': date.isocalendar()[1],
            'day': date.day,
            'day_of_week': date.dayofweek,
            'day_name': date.strftime('%A'),
            'is_weekend': 1 if date.dayofweek >= 5 else 0,
            'year_month': date.strftime('%Y-%m')
        })
    
    dim_date_df = pd.DataFrame(dim_date_data)
    dim_date_df.to_sql('dim_date', conn, if_exists='replace', index=False)
    print(f"  ✓ Loaded {len(dim_date_df)} date records")
    
    return dim_date_df

def populate_dimensions(conn, df):
    """Populate all dimension tables"""
    print("\n" + "=" * 70)
    print("STEP 3: POPULATING DIMENSION TABLES")
    print("=" * 70)
    
    # Populate dim_product
    print("→ Populating dim_product...")
    dim_product = df[['Product_ID','Product_Key', 'Product_Name', 'Category', 'Brand', 'Model', 'Product_Type', 'Unit_Price']].drop_duplicates('Product_ID')
    dim_product.columns = ['product_id', 'product_key','product_name', 'category', 'brand', 'model', 'product_type', 'unit_price']
    dim_product.to_sql('dim_product', conn, if_exists='replace', index=False)
    
    # Get product_key mapping
    product_mapping = pd.read_sql("SELECT product_id FROM dim_product", conn)
    print(f"  ✓ Loaded {len(dim_product)} product records")
    
    # Populate dim_customer
    print("→ Populating dim_customer...")
    dim_customer = df[['Customer_ID', 'Customer_Key','Customer_Age', 'Customer_Gender', 'Age_Group']].drop_duplicates('Customer_ID')
    dim_customer.columns = ['customer_id', 'customer_key','customer_age', 'customer_gender', 'age_group']
    dim_customer.to_sql('dim_customer', conn, if_exists='replace', index=False)
    
    # Get customer_key mapping
    customer_mapping = pd.read_sql("SELECT customer_key, customer_id FROM dim_customer", conn)
    print(f"  ✓ Loaded {len(dim_customer)} customer records")
    
    # Populate dim_date
    dim_date = populate_dim_date(conn, df)
    
    # Populate dim_staff
    print("→ Populating dim_staff...")
    dim_staff = df[['Staff_ID', 'Staff_Key']].drop_duplicates()
    dim_staff.columns = ['staff_id', 'staff_key']
    dim_staff.to_sql('dim_staff', conn, if_exists='replace', index=False)
    
    # Get staff_key mapping
    staff_mapping = pd.read_sql("SELECT staff_key, staff_id FROM dim_staff", conn)
    print(f"  ✓ Loaded {len(dim_staff)} staff records")
    
    # Populate dim_supplier
    print("→ Populating dim_supplier...")
    dim_supplier = df[['Supplier_ID', 'Supplier_Key']].drop_duplicates()
    dim_supplier.columns = ['supplier_id', 'supplier_key']
    dim_supplier.to_sql('dim_supplier', conn, if_exists='replace', index=False)
    
    # Get supplier_key mapping
    supplier_mapping = pd.read_sql("SELECT supplier_key, supplier_id FROM dim_supplier", conn)
    print(f"  ✓ Loaded {len(dim_supplier)} supplier records")
    
    return product_mapping, customer_mapping, dim_date, staff_mapping, supplier_mapping

def populate_fact_table(conn, df, product_mapping, customer_mapping, dim_date, staff_mapping, supplier_mapping):
    """Populate the fact table"""
    print("\n" + "=" * 70)
    print("STEP 4: POPULATING FACT TABLE")
    print("=" * 70)
    
    print("→ Creating fact_transactions...")

    try:
        # Merge to get foreign keys
        fact = df.copy()

        # Add product_key
        product_mapping = product_mapping.drop_duplicates('product_id')
        fact = fact.merge(product_mapping, left_on='Product_ID', right_on='product_id', how='left', validate='many_to_one').drop_duplicates()
        
        # Add customer_key
        customer_mapping = customer_mapping.drop_duplicates('customer_id')
        fact = fact.merge(customer_mapping, left_on='Customer_ID', right_on='customer_id', how='left', validate='many_to_one').drop_duplicates()
        
        # Add date_key
        fact['date_key'] = fact['Transaction_DateTime'].dt.strftime('%Y%m%d').astype(int).drop_duplicates()
        
        # Add staff_key
        staff_mapping = staff_mapping.drop_duplicates('staff_id')
        fact = fact.merge(staff_mapping, left_on='Staff_ID', right_on='staff_id', how='left', validate='many_to_one').drop_duplicates()

        # Add supplier_key
        supplier_mapping = supplier_mapping.drop_duplicates('supplier_id')
        fact = fact.merge(supplier_mapping, left_on='Supplier_ID', right_on='supplier_id', how='left', validate='many_to_one').drop_duplicates()

        # Select and rename columns for fact table
        fact_table = fact[[
            'Product_Key', 'Customer_Key', 'date_key', 'Staff_Key', 'Supplier_Key',
            'Transaction_ID', 'Transaction_DateTime', 'Order_Type', 'Payment_Method', 'Transaction_Status',
            'Quantity', 'Total_Amount', 'Discount_Applied', 'Delivery_Time_Min', 'Customer_Rating', 'Inventory_Level'
        ]].copy()

        fact_table.columns = [
            'product_key', 'customer_key', 'date_key', 'staff_key', 'supplier_key',
            'transaction_id', 'transaction_datetime', 'order_type', 'payment_method', 'transaction_status',
            'quantity', 'total_amount', 'discount_applied', 'delivery_time_min', 'customer_rating', 'inventory_level'
        ]

        # Load to database
        fact_table.to_sql('fact_transactions', conn, if_exists='replace', index=False)
        print(f"  ✓ Loaded {len(fact_table)} transaction records")
    except Exception as e:
        print(e)

def create_aggregate_tables(conn):
    """Create pre-aggregated summary tables for dashboard performance"""
    print("\n" + "=" * 70)
    print("STEP 5: CREATING AGGREGATE TABLES")
    print("=" * 70)
    
    cursor = conn.cursor()
    
    cursor.execute("BEGIN")

    try:
        # Aggregate 1: KPI Revenue by Dimension
        print("→ Creating agg_kpi_revenue_by_dimension...")
        cursor.execute("""
            INSERT INTO agg_kpi_revenue_by_dimension
            SELECT 'Category' as dimension, p.category as dimension_value,
                SUM(f.total_amount) as total_amount,
                COUNT(*) as transaction_count,
                AVG(f.total_amount) as avg_transaction_value
            FROM fact_transactions f
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE f.transaction_status = 'Completed'
            GROUP BY p.category
            
            UNION ALL
            
            SELECT 'Brand' as dimension, p.brand as dimension_value,
                SUM(f.total_amount) as total_amount,
                COUNT(*) as transaction_count,
                AVG(f.total_amount) as avg_transaction_value
            FROM fact_transactions f
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE f.transaction_status = 'Completed'
            GROUP BY p.brand
            
            UNION ALL
            
            SELECT 'Model' as dimension, p.model as dimension_value,
                SUM(f.total_amount) as total_amount,
                COUNT(*) as transaction_count,
                AVG(f.total_amount) as avg_transaction_value
            FROM fact_transactions f
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE f.transaction_status = 'Completed'
            GROUP BY p.model
        """)
        print("  ✓ Created KPI revenue aggregates")
        
        # Aggregate 2: Transaction Status by Order Type
        print("→ Creating agg_kpi_status_by_order_type...")
        cursor.execute("""
            INSERT INTO agg_kpi_status_by_order_type
            SELECT order_type, transaction_status, COUNT(*) as record_count
            FROM fact_transactions
            GROUP BY order_type, transaction_status
        """)
        print("  ✓ Created order status aggregates")
        
        # Aggregate 3: Customer Metrics
        print("→ Creating agg_customer_metrics...")
        cursor.execute("""
            INSERT INTO agg_customer_metrics
            SELECT c.age_group, c.customer_gender as gender, d.year_month,
                AVG(f.discount_applied) as avg_discount_applied,
                AVG(f.customer_rating) as avg_customer_rating,
                COUNT(*) as transaction_count,
                SUM(f.total_amount) as total_revenue
            FROM fact_transactions f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.transaction_status = 'Completed'
            GROUP BY c.age_group, c.customer_gender, d.year_month
        """)
        print("  ✓ Created customer metrics aggregates")
        
        # Aggregate 4: Product Type Distribution
        print("→ Creating agg_product_type_distribution...")
        cursor.execute("""
            INSERT INTO agg_product_type_distribution
            SELECT 'Category' as dimension, p.category as dimension_value, p.product_type,
                COUNT(*) as record_count, SUM(f.total_amount) as total_revenue
            FROM fact_transactions f
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE f.transaction_status = 'Completed'
            GROUP BY p.category, p.product_type
            
            UNION ALL
            
            SELECT 'Brand' as dimension, p.brand as dimension_value, p.product_type,
                COUNT(*) as record_count, SUM(f.total_amount) as total_revenue
            FROM fact_transactions f
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE f.transaction_status = 'Completed'
            GROUP BY p.brand, p.product_type
            
            UNION ALL
            
            SELECT 'Model' as dimension, p.model as dimension_value, p.product_type,
                COUNT(*) as record_count, SUM(f.total_amount) as total_revenue
            FROM fact_transactions f
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE f.transaction_status = 'Completed'
            GROUP BY p.model, p.product_type
            
            UNION ALL
            
            SELECT 'Product_Name' as dimension, p.product_name as dimension_value, p.product_type,
                COUNT(*) as record_count, SUM(f.total_amount) as total_revenue
            FROM fact_transactions f
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE f.transaction_status = 'Completed'
            GROUP BY p.product_name, p.product_type
        """)
        print("  ✓ Created product type distribution aggregates")
        conn.commit()

    except Exception as e:
        print(e)
        conn.rollback()
    

def create_indexes(conn):
    """Create indexes for better query performance"""
    print("\n" + "=" * 70)
    print("STEP 6: CREATING INDEXES")
    print("=" * 70)
    
    cursor = conn.cursor()
    
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_transactions(product_key)",
        "CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_transactions(customer_key)",
        "CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_transactions(date_key)",
        "CREATE INDEX IF NOT EXISTS idx_fact_order_type ON fact_transactions(order_type)",
        "CREATE INDEX IF NOT EXISTS idx_fact_status ON fact_transactions(transaction_status)",
        "CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dim_product(category)",
        "CREATE INDEX IF NOT EXISTS idx_dim_product_brand ON dim_product(brand)",
        "CREATE INDEX IF NOT EXISTS idx_dim_customer_age_group ON dim_customer(age_group)"
    ]
    
    for idx_sql in indexes:
        cursor.execute(idx_sql)
    
    conn.commit()
    print("  ✓ All indexes created")

def display_statistics(conn):
    """Display database statistics"""
    print("\n" + "=" * 70)
    print("DATA WAREHOUSE STATISTICS")
    print("=" * 70)
    
    cursor = conn.cursor()
    
    tables = [
        'dim_product', 'dim_customer', 'dim_date', 'dim_staff', 'dim_supplier',
        'fact_transactions',
        'agg_kpi_revenue_by_dimension', 'agg_kpi_status_by_order_type',
        'agg_customer_metrics', 'agg_product_type_distribution'
    ]
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:.<40} {count:>8,} records")

def main():
    """Main ETL pipeline execution"""
    print("\n" + "=" * 70)
    print("MOBILE SHOP ETL PIPELINE - STAR SCHEMA")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Extract
    df_raw = extract_data()
    
    # Transform
    df_transformed = transform_data(df_raw)
    
    # Load
    try:
        print(f"\n→ Connecting to database: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        
        # Populate dimensions
        product_mapping, customer_mapping, dim_date, staff_mapping, supplier_mapping = populate_dimensions(conn, df_transformed)
        
        # Populate fact table
        populate_fact_table(conn, df_transformed, product_mapping, customer_mapping, dim_date, staff_mapping, supplier_mapping)
        
        # Create aggregates
        create_aggregate_tables(conn)
        
        # Create indexes
        create_indexes(conn)
        
        # Display statistics
        display_statistics(conn)
        
        conn.close()
        
        print("\n" + "=" * 70)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Data warehouse: {DB_PATH}")
        print("\nYou can now run the dashboard:")
        print("  python dashboard.py")
        print("=" * 70 + "\n")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

