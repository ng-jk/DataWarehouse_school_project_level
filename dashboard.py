import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sqlite3

# Configuration
DB_PATH = "mobile_shop_dw.db"

# Initialize Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Mobile Shop Performance Dashboard"

# Color scheme
COLORS = {
    'primary': '#1976D2',
    'secondary': '#2E7D32',
    'accent': '#FF6B6B',
    'background': '#F5F5F5',
    'card': '#FFFFFF',
    'text': '#333333'
}

def get_data(query):
    """Execute SQL query and return DataFrame"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ============================================================================
# LAYOUT
# ============================================================================

app.layout = html.Div(style={'backgroundColor': COLORS['background'], 'fontFamily': 'Arial, sans-serif'}, children=[
    
    # Header
    html.Div(style={'backgroundColor': COLORS['primary'], 'padding': '20px', 'marginBottom': '20px'}, children=[
        html.H1("Mobile Shop Performance Dashboard", 
                style={'color': 'white', 'margin': '0', 'textAlign': 'center'}),
        html.P("Real-time Business Intelligence & Analytics",
               style={'color': 'white', 'margin': '5px 0 0 0', 'textAlign': 'center', 'fontSize': '14px'})
    ]),
    
    # Main content
    html.Div(style={'padding': '0 20px'}, children=[
        
        # Tabs
        dcc.Tabs(id='main-tabs', value='kpi-tab', children=[
            
            # Tab 1: KPI Analysis
            dcc.Tab(label=' KPI Analysis', value='kpi-tab', style={'fontWeight': 'bold'}),
            
            # Tab 2: Customer Insights
            dcc.Tab(label=' Customer Insights', value='customer-tab', style={'fontWeight': 'bold'}),
            
            # Tab 3: Product Deep-Dive
            dcc.Tab(label=' Product Deep-Dive', value='product-tab', style={'fontWeight': 'bold'}),
        ]),
        
        # Tab content
        html.Div(id='tab-content', style={'marginTop': '20px'})
    ])
])

# ============================================================================
# CALLBACKS
# ============================================================================

@app.callback(
    Output('tab-content', 'children'),
    Input('main-tabs', 'value')
)
def render_tab_content(tab):
    """Render content based on selected tab"""
    
    if tab == 'kpi-tab':
        return render_kpi_tab()
    elif tab == 'customer-tab':
        return render_customer_tab()
    elif tab == 'product-tab':
        return render_product_tab()

# ============================================================================
# TAB 1: KPI ANALYSIS
# ============================================================================

def render_kpi_tab():
    """Render KPI Analysis tab"""
    
    # Get data
    kpi_data = get_data("SELECT * FROM agg_kpi_revenue_by_dimension")
    status_data = get_data("SELECT * FROM agg_kpi_status_by_order_type")

    print(kpi_data)
    
    # Filter by dimension
    category_data = kpi_data[kpi_data['dimension'] == 'Category'].sort_values('total_amount', ascending=False)
    brand_data = kpi_data[kpi_data['dimension'] == 'Brand'].sort_values('total_amount', ascending=False)
    model_data = kpi_data[kpi_data['dimension'] == 'Model'].sort_values('total_amount', ascending=False).head(10)
    
    # Chart 1: Revenue by Category
    fig_category = px.bar(
        category_data,
        x='dimension_value',
        y='total_amount',
        title='Total Revenue by Category',
        labels={'dimension_value': 'Category', 'total_amount': 'Total Amount (RM)'},
        color='total_amount',
        color_continuous_scale='Blues'
    )
    fig_category.update_layout(showlegend=False)
    
    # Chart 2: Revenue by Brand
    fig_brand = px.bar(
        brand_data,
        x='dimension_value',
        y='total_amount',
        title='Total Revenue by Brand',
        labels={'dimension_value': 'Brand', 'total_amount': 'Total Amount (RM)'},
        color='total_amount',
        color_continuous_scale='Greens'
    )
    fig_brand.update_layout(showlegend=False)
    
    # Chart 3: Top 10 Models
    fig_model = px.bar(
        model_data,
        y='dimension_value',
        x='total_amount',
        title='Top 10 Models by Revenue',
        labels={'dimension_value': 'Model', 'total_amount': 'Total Amount (RM)'},
        orientation='h',
        color='total_amount',
        color_continuous_scale='Oranges'
    )
    fig_model.update_layout(showlegend=False, yaxis={'categoryorder': 'total ascending'})
    
    # Chart 4: Transaction Status by Order Type
    fig_status = px.bar(
        status_data,
        x='order_type',
        y='record_count',
        color='transaction_status',
        title='Transaction Status by Order Type',
        labels={'order_type': 'Order Type', 'record_count': 'Number of Transactions', 'transaction_status': 'Status'},
        barmode='group',
        color_discrete_map={'Completed': '#4CAF50', 'Cancelled': '#F44336', 'Refunded': '#FFC107'}
    )
    
    return html.Div([
        html.Div([
            html.Div([
                dcc.Graph(figure=fig_category)
            ], style={'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px', 'margin': 'auto'}),
            
            html.Div([
                dcc.Graph(figure=fig_brand)
            ], style={'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px'}),
        ]),
        
        html.Div([
            html.Div([
                dcc.Graph(figure=fig_model)
            ], style={'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px', 'margin': 'auto'}),
            
            html.Div([
                dcc.Graph(figure=fig_status)
            ], style={ 'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px'}),
        ], style={'marginTop': '20px'})
    ])

# ============================================================================
# TAB 2: CUSTOMER INSIGHTS
# ============================================================================

def render_customer_tab():
    """Render Customer Insights tab"""
    
    # Get data
    customer_data = get_data("SELECT * FROM agg_customer_metrics")
    
    # Aggregate by age group and gender
    discount_data = customer_data.groupby(['age_group', 'gender'])['avg_discount_applied'].mean().reset_index()
    rating_data = customer_data.groupby(['age_group', 'gender'])['avg_customer_rating'].mean().reset_index()
    
    # Time series data
    monthly_data = customer_data.groupby('year_month')['transaction_count'].sum().reset_index()
    monthly_data = monthly_data.sort_values('year_month')
    
    # Chart 1: Average Discount by Customer Group
    fig_discount = px.bar(
        discount_data,
        x='age_group',
        y='avg_discount_applied',
        color='gender',
        title='Average Discount Applied by Customer Demographics',
        labels={'age_group': 'Age Group', 'avg_discount_applied': 'Avg Discount (RM)', 'gender': 'Gender'},
        barmode='group',
        color_discrete_map={'Male': '#2196F3', 'Female': '#E91E63'}
    )
    
    # Chart 2: Average Rating by Customer Group
    fig_rating = px.bar(
        rating_data,
        x='age_group',
        y='avg_customer_rating',
        color='gender',
        title='Average Customer Rating by Demographics',
        labels={'age_group': 'Age Group', 'avg_customer_rating': 'Avg Rating', 'gender': 'Gender'},
        barmode='group',
        color_discrete_map={'Male': '#2196F3', 'Female': '#E91E63'}
    )
    
    # Chart 3: Transaction Trends Over Time
    fig_trend = px.line(
        monthly_data,
        x='year_month',
        y='transaction_count',
        title='Transaction Trends Over Time',
        labels={'year_month': 'Month', 'transaction_count': 'Number of Transactions'},
        markers=True
    )
    fig_trend.update_traces(line_color='#1976D2', line_width=3)
    
    # Chart 4: Customer Segmentation Heatmap
    heatmap_data = customer_data.groupby(['age_group', 'gender'])['total_revenue'].sum().reset_index()
    heatmap_pivot = heatmap_data.pivot(index='age_group', columns='gender', values='total_revenue')
    
    fig_heatmap = go.Figure(data=go.Heatmap(
        z=heatmap_pivot.values,
        x=heatmap_pivot.columns,
        y=heatmap_pivot.index,
        colorscale='YlOrRd',
        text=heatmap_pivot.values,
        texttemplate='RM %{text:,.0f}',
        textfont={"size": 12}
    ))
    fig_heatmap.update_layout(
        title='Total Revenue by Customer Segment',
        xaxis_title='Gender',
        yaxis_title='Age Group'
    )
    
    return html.Div([
        html.Div([
            html.Div([
                dcc.Graph(figure=fig_discount)
            ], style={'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px', 'margin': 'auto'}),
            
            html.Div([
                dcc.Graph(figure=fig_rating)
            ], style={ 'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px'}),
        ]),
        
        html.Div([
            html.Div([
                dcc.Graph(figure=fig_trend)
            ], style={ 'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px', 'margin': 'auto'}),
            
            html.Div([
                dcc.Graph(figure=fig_heatmap)
            ], style={ 'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px'}),
        ], style={'marginTop': '20px'})
    ])

# ============================================================================
# TAB 3: PRODUCT DEEP-DIVE
# ============================================================================

def render_product_tab():
    """Render Product Deep-Dive tab"""
    
    # Get data
    product_data = get_data("SELECT * FROM agg_product_type_distribution")
    
    # Filter by dimension
    category_dist = product_data[product_data['dimension'] == 'Category']
    brand_dist = product_data[product_data['dimension'] == 'Brand']
    model_dist = product_data[product_data['dimension'] == 'Model'].groupby(['dimension_value', 'product_type'])['record_count'].sum().reset_index()
    model_dist = model_dist.sort_values('record_count', ascending=False).head(20)
    product_name_dist = product_data[product_data['dimension'] == 'Product_Name'].groupby(['dimension_value', 'product_type'])['record_count'].sum().reset_index()
    product_name_dist = product_name_dist.sort_values('record_count', ascending=False).head(20)
    
    # Chart 1: Product Type by Category
    fig_category = px.bar(
        category_dist,
        x='dimension_value',
        y='record_count',
        color='product_type',
        title='Product Type Distribution by Category',
        labels={'dimension_value': 'Category', 'record_count': 'Number of Transactions', 'product_type': 'Product Type'},
        barmode='stack',
        color_discrete_map={'Accessory': '#4CAF50', 'Repair': '#2196F3'}
    )
    
    # Chart 2: Product Type by Brand
    fig_brand = px.bar(
        brand_dist,
        x='dimension_value',
        y='record_count',
        color='product_type',
        title='Product Type Distribution by Brand',
        labels={'dimension_value': 'Brand', 'record_count': 'Number of Transactions', 'product_type': 'Product Type'},
        barmode='stack',
        color_discrete_map={'Accessory': '#4CAF50', 'Repair': '#2196F3'}
    )
    
    # Chart 3: Top 10 Models
    fig_model = px.bar(
        model_dist.head(10),
        y='dimension_value',
        x='record_count',
        color='product_type',
        title='Top 10 Models by Transaction Count',
        labels={'dimension_value': 'Model', 'record_count': 'Number of Transactions', 'product_type': 'Product Type'},
        orientation='h',
        barmode='stack',
        color_discrete_map={'Accessory': '#4CAF50', 'Repair': '#2196F3'}
    )
    fig_model.update_layout(yaxis={'categoryorder': 'total ascending'})
    
    # Chart 4: Top 10 Product Names
    fig_product = px.bar(
        product_name_dist.head(10),
        y='dimension_value',
        x='record_count',
        color='product_type',
        title='Top 10 Products by Transaction Count',
        labels={'dimension_value': 'Product Name', 'record_count': 'Number of Transactions', 'product_type': 'Product Type'},
        orientation='h',
        barmode='stack',
        color_discrete_map={'Accessory': '#4CAF50', 'Repair': '#2196F3'}
    )
    fig_product.update_layout(yaxis={'categoryorder': 'total ascending'})
    
    return html.Div([
        html.Div([
            html.Div([
                dcc.Graph(figure=fig_category)
            ], style={ 'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px', 'margin': 'auto'}),
            
            html.Div([
                dcc.Graph(figure=fig_brand)
            ], style={'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px'}),
        ]),
        
        html.Div([
            html.Div([
                dcc.Graph(figure=fig_model)
            ], style={'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px', 'margin': 'auto'}),
            
            html.Div([
                dcc.Graph(figure=fig_product)
            ], style={'display': 'block', 'backgroundColor': COLORS['card'], 'padding': '10px', 'borderRadius': '5px'}),
        ], style={'marginTop': '20px'})
    ])

# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == '__main__':
    print("\n" + "=" * 70)
    print("MOBILE SHOP BI DASHBOARD")
    print("=" * 70)
    print("Starting dashboard server...")
    print("Dashboard URL: http://127.0.0.1:8050")
    print("Press Ctrl+C to stop the server")
    print("=" * 70 + "\n")
    
    app.run(debug=True, host='127.0.0.1', port=8050)

