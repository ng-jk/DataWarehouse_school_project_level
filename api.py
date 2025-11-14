import os
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import pandas as pd
from typing import Optional
import uvicorn
import sqlite3

# Connect to (or create) the SQLite database
conn = sqlite3.connect("mobile_shop_dw.db")
cursor = conn.cursor()

def load_data():
    """Load the CSV data into memory"""
    try:
        cursor.execute("SELECT filename FROM csv_readed")
        read_files = {row[0] for row in cursor.fetchall()}  # set for fast lookup

        for file in os.listdir("datasource"):
            if file.endswith(".csv") and file not in read_files:
                file_path = os.path.join('datasource', file)
                print(f"Loading new CSV: {file}")

                # Load CSV
                data = pd.read_csv(file_path)
                break
        
        # stop executing if no file found
        if data is None or data.empty:
            return None

        data = pd.read_csv('mobile_shop_transactions_3years.csv')
        # Convert datetime column
        data['Transaction_DateTime'] = pd.to_datetime(
            data['Transaction_DateTime'], 
            format='%d/%m/%Y %H:%M'
        )
        # Convert to string for JSON serialization
        data['Transaction_DateTime'] = data['Transaction_DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data['file_name'] = file_path
        print(f"Data loaded successfully: {len(data)} records")
    except Exception as e:
        print(f"Error loading data: {e}")
        data = pd.DataFrame()
    return data

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.data = load_data()
    yield   

# Initialize FastAPI app
app = FastAPI(
    title="Mobile Shop Data API",
    description="API for accessing mobile shop transaction data",
    version="1.0.0",
    lifespan=lifespan,
)

#index of the app
@app.get("/")
async def root():
    data = app.state.data
    return {
        "message": "Mobile Shop Data API",
        "version": "1.0.0",
        "endpoints": {
            "/transactions": "Get all transaction data",
            "/transactions/stats": "Get data statistics",
            "/docs": "API documentation"
        }
    }

# transaction
@app.get("/transactions")
async def get_transactions(
        limit: Optional[int] = Query(None, description="Limit the number of records returned"),
        offset: Optional[int] = Query(0, description="Number of records to skip"),
        category: Optional[str] = Query(None, description="Filter by category"),
        brand: Optional[str] = Query(None, description="Filter by brand"),
        order_type: Optional[str] = Query(None, description="Filter by order type")
    ):

    data = app.state.data

    if data is None or data.empty:
        return JSONResponse(
            status_code=500,
            content={"error": "Data not loaded"}
        )
    
    # Apply filters
    filtered_data = data.copy()
    
    if category:
        filtered_data = filtered_data[filtered_data['Category'] == category]
    
    if brand:
        filtered_data = filtered_data[filtered_data['Brand'] == brand]
    
    if order_type:
        filtered_data = filtered_data[filtered_data['Order_Type'] == order_type]
    
    # Apply pagination
    if offset:
        filtered_data = filtered_data.iloc[offset:]
    
    if limit:
        filtered_data = filtered_data.head(limit)
    
    # Convert to JSON
    records = filtered_data.to_dict(orient='records')
    
    return {
        "total_records": len(data),
        "filtered_records": len(filtered_data),
        "returned_records": len(records),
        "data": records
    }

@app.get("/transactions/stats")
async def get_stats():
    data = app.state.data

    if data is None or data.empty:
        return JSONResponse(
            status_code=500,
            content={"error": "Data not loaded"}
        )
    
    stats = {
        "total_records": len(data),
        "total_revenue": float(data['Total_Amount'].sum()),
        "average_transaction": float(data['Total_Amount'].mean()),
        "date_range": {
            "start": data['Transaction_DateTime'].min(),
            "end": data['Transaction_DateTime'].max()
        },
        "categories": data['Category'].unique().tolist(),
        "brands": data['Brand'].unique().tolist(),
        "order_types": data['Order_Type'].unique().tolist(),
        "transaction_status": data['Transaction_Status'].value_counts().to_dict()
    }
    
    return stats

@app.get("/transactions/categories")
async def get_categories():
    data = app.state.data
    if data is None or data.empty:
        return JSONResponse(status_code=500, content={"error": "Data not loaded"})
    
    return {"categories": data['Category'].unique().tolist()}

@app.get("/transactions/brands")
async def get_brands():
    data = app.state.data
    if data is None or data.empty:
        return JSONResponse(status_code=500, content={"error": "Data not loaded"})
    
    return {"brands": data['Brand'].unique().tolist()}

if __name__ == "__main__":
    # Run the API server
    print("=" * 60)
    print("Mobile Shop Data API")
    print("=" * 60)
    print("Starting server on http://127.0.0.1:8000")
    print("API Documentation: http://127.0.0.1:8000/docs")
    print("=" * 60)
    
    uvicorn.run(app, host="127.0.0.1", port=8000)

