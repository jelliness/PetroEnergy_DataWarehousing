import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

# Database connection parameters - update these with your actual values
DB_PARAMS = {
    'dbname': 'Petroenergy_Data_Warehousing',
    'user': 'postgres',
    'password': 'Papasa01!',
    'host': 'localhost',
    'port': '5432'
}

def get_db_connection():
    """
    Create and return a database connection.
    """
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def query_economic_value_by_year():
    """Query economic value summary by year from database."""
    try:
        conn = get_db_connection()
        if conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT * FROM gold.func_economic_value_by_year()")
                results = cursor.fetchall()
                conn.close()
                df = pd.DataFrame(results)
                print("\n=== Economic Value by Year Data ===")
                print(df)
                return df
        return None
    except Exception as e:
        print(f"Query error: {e}")
        return None

def query_economic_value_distributed_details(year=None):
    """Query economic value distribution details."""
    try:
        conn = get_db_connection()
        if conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if year:
                    cursor.execute("SELECT * FROM gold.func_economic_value_distributed_details(ARRAY[%s]::SMALLINT[])", (year,))
                else:
                    cursor.execute("SELECT * FROM gold.func_economic_value_distributed_details()")
                results = cursor.fetchall()
                conn.close()
                df = pd.DataFrame(results)
                print(f"\n=== Economic Value Distribution Details {year if year else 'All Years'} ===")
                print(df)
                return df
        return None
    except Exception as e:
        print(f"Query error: {e}")
        return None

def query_economic_value_distribution_percentage(year=None):
    """Query company distribution percentages."""
    try:
        conn = get_db_connection()
        if conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if year:
                    cursor.execute("SELECT * FROM gold.func_economic_value_distribution_percentage(NULL, ARRAY[%s]::SMALLINT[])", (year,))
                else:
                    cursor.execute("SELECT * FROM gold.func_economic_value_distribution_percentage()")
                results = cursor.fetchall()
                conn.close()
                df = pd.DataFrame(results)
                print(f"\n=== Economic Value Distribution Percentage {year if year else 'All Years'} ===")
                print(df)
                return df
        return None
    except Exception as e:
        print(f"Query error: {e}")
        return None

def query_economic_value_generated_details():
    """Query economic value generation details."""
    try:
        conn = get_db_connection()
        if conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT * FROM gold.func_economic_value_generated_details()")
                results = cursor.fetchall()
                conn.close()
                df = pd.DataFrame(results)
                print("\n=== Economic Value Generated Details ===")
                print(df)
                return df
        return None
    except Exception as e:
        print(f"Query error: {e}")
        return None

def query_economic_expenditure_by_company(year=None):
    """Query company expenditure details."""
    try:
        conn = get_db_connection()
        if conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if year:
                    cursor.execute("SELECT * FROM gold.func_economic_expenditure_by_company(NULL, NULL, ARRAY[%s]::SMALLINT[])", (year,))
                else:
                    cursor.execute("SELECT * FROM gold.func_economic_expenditure_by_company()")
                results = cursor.fetchall()
                conn.close()
                df = pd.DataFrame(results)
                print(f"\n=== Economic Expenditure by Company {year if year else 'All Years'} ===")
                print(df)
                return df
        return None
    except Exception as e:
        print(f"Query error: {e}")
        return None

def query_stakeholder_distribution(year=None):
    """Query distribution by stakeholder group."""
    try:
        conn = get_db_connection()
        if conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if year:
                    cursor.execute("""
                        SELECT 'Government' as stakeholder_group, total_government_payments as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[%s]::SMALLINT[])
                        UNION ALL
                        SELECT 'Suppliers (Local)' as stakeholder_group, total_local_supplier_spending as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[%s]::SMALLINT[])
                        UNION ALL
                        SELECT 'Suppliers (Foreign)' as stakeholder_group, total_foreign_supplier_spending as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[%s]::SMALLINT[])
                        UNION ALL
                        SELECT 'Employees' as stakeholder_group, total_employee_wages_benefits as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[%s]::SMALLINT[])
                        UNION ALL
                        SELECT 'Communities' as stakeholder_group, total_community_investments as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[%s]::SMALLINT[])
                        UNION ALL
                        SELECT 'Capital Providers' as stakeholder_group, total_capital_provider_payments as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[%s]::SMALLINT[])
                    """, (year, year, year, year, year, year))
                else:
                    # Use most recent year if none specified
                    cursor.execute("""
                        WITH recent_year AS (
                            SELECT MAX(year) as year FROM gold.vw_economic_value_distributed
                        )
                        SELECT 'Government' as stakeholder_group, total_government_payments as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[(SELECT year FROM recent_year)]::SMALLINT[])
                        UNION ALL
                        SELECT 'Suppliers (Local)' as stakeholder_group, total_local_supplier_spending as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[(SELECT year FROM recent_year)]::SMALLINT[])
                        UNION ALL
                        SELECT 'Suppliers (Foreign)' as stakeholder_group, total_foreign_supplier_spending as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[(SELECT year FROM recent_year)]::SMALLINT[])
                        UNION ALL
                        SELECT 'Employees' as stakeholder_group, total_employee_wages_benefits as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[(SELECT year FROM recent_year)]::SMALLINT[])
                        UNION ALL
                        SELECT 'Communities' as stakeholder_group, total_community_investments as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[(SELECT year FROM recent_year)]::SMALLINT[])
                        UNION ALL
                        SELECT 'Capital Providers' as stakeholder_group, total_capital_provider_payments as value
                        FROM gold.func_economic_value_distributed_details(ARRAY[(SELECT year FROM recent_year)]::SMALLINT[])
                    """)
                results = cursor.fetchall()
                conn.close()
                df = pd.DataFrame(results)
                print(f"\n=== Stakeholder Distribution {year if year else 'Most Recent Year'} ===")
                print(df)
                return df
        return None
    except Exception as e:
        print(f"Query error: {e}")
        return None

def calculate_retention_ratio():
    """Calculate economic value retention ratio from summary data."""
    try:
        df = query_economic_value_by_year()
        if df is not None and not df.empty:
            # Convert Decimal columns to float for calculations
            df = df.astype({
                'total_economic_value_generated': float,
                'economic_value_retained': float
            })
            df['retention_ratio'] = df['economic_value_retained'] / df['total_economic_value_generated']
            print("\n=== Retention Ratio Calculation ===")
            print(df[['year', 'retention_ratio']])
            return df[['year', 'retention_ratio']]
        return None
    except Exception as e:
        print(f"Calculation error: {e}")
        return None

def economic_value_flow_by_year():
    """
    Create bar chart showing economic value generated vs distributed by year.
    """
    # Get data from database
    data = query_economic_value_by_year()
    
    if data is None or data.empty:
        print("No data available for economic value flow visualization")
        return None
    
    # Convert Decimal columns to float
    numeric_columns = ['total_economic_value_generated', 'total_economic_value_distributed', 'economic_value_retained']
    data = data.astype({col: float for col in numeric_columns})
    
    # Create the visualization
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Convert to long format for grouped bars
    data_melted = pd.melt(
        data, 
        id_vars=['year'], 
        value_vars=numeric_columns,
        var_name='category', value_name='value'
    )
    
    # Plot the data
    sns.barplot(x='year', y='value', hue='category', data=data_melted, ax=ax)
    
    # Customize the plot
    plt.title('Economic Value Flow by Year', fontsize=16)
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Value (in thousands)', fontsize=12)
    plt.legend(title='Category', fontsize=10)
    
    # Format y-axis with commas and PHP symbol
    formatter = ticker.StrMethodFormatter('₱{x:,.0f}')
    ax.yaxis.set_major_formatter(formatter)
    
    # Set x-axis to have integer interval of 1
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    
    plt.tight_layout()
    return fig

def economic_value_distribution_pie(year=2023):
    """
    Create pie chart showing distribution of economic value for a specific year.
    Excludes "other" expenditures which aren't part of economic value distributed.
    """
    # Get data from database
    raw_data = query_economic_value_distributed_details(year)
    
    if raw_data is None or raw_data.empty:
        print(f"No data available for economic value distribution pie chart for year {year}")
        return None
    
    # Get the first row (for the specified year)
    row = raw_data.iloc[0]
    
    # Convert all values to float - exclude 'total_other_expenditures'
    distribution_data = {
        'category': ['Government Payments', 'Local Supplier Spending', 
                    'Foreign Supplier Spending', 'Employee Wages & Benefits', 
                    'Community Investments', 'Capital Provider Payments'],
        'value': [
            float(row['total_government_payments']), 
            float(row['total_local_supplier_spending']), 
            float(row['total_foreign_supplier_spending']),
            float(row['total_employee_wages_benefits']),
            float(row['total_community_investments']),
            float(row['total_capital_provider_payments'])
        ]
    }
    data = pd.DataFrame(distribution_data)
    
    # Create pie chart
    fig, ax = plt.subplots(figsize=(10, 8))
    colors = sns.color_palette('pastel')
    wedges, texts, autotexts = plt.pie(
        data['value'], 
        labels=data['category'], 
        autopct='%1.1f%%', 
        startangle=90, 
        colors=colors
    )
    
    # Customize text
    plt.setp(autotexts, size=9, weight="bold")
    plt.axis('equal')
    plt.title(f'Economic Value Distribution {year}', fontsize=16)
    plt.tight_layout()
    return fig

def top_contributing_companies(year=2023):
    """
    Create horizontal bar chart showing top contributing companies.
    """
    # Get data from database
    data = query_economic_value_distribution_percentage(year)
    
    if data is None or data.empty:
        print(f"No data available for top contributing companies for year {year}")
        return None
    
    # Convert percentage to float
    if 'percentage_of_total_distribution' in data.columns:
        data['percentage_of_total_distribution'] = data['percentage_of_total_distribution'].astype(float)
    
    # Sort data by percentage
    data = data.sort_values('percentage_of_total_distribution', ascending=False)
    
    # Create horizontal bar chart
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Fix the barplot call to avoid FutureWarning
    sns.barplot(
        x='percentage_of_total_distribution', 
        y='company_name', 
        hue='company_name', 
        legend=False, 
        data=data, 
        ax=ax
    )
    
    # Add labels and title
    plt.title(f'Top Contributing Companies - Economic Value Distribution ({year})', fontsize=16)
    plt.xlabel('Percentage of Total Economic Value Distribution', fontsize=12)
    plt.ylabel('')
    
    # Add percentage labels
    for i, v in enumerate(data['percentage_of_total_distribution']):
        v_float = float(v)
        ax.text(v_float + 0.5, i, f"{v_float:.1f}%", va='center')
    
    plt.tight_layout()
    return fig

def economic_value_generated_trend():
    """
    Create line chart showing components of economic value generated over time.
    """
    # Get data from database
    data = query_economic_value_generated_details()
    
    if data is None or data.empty:
        print("No data available for economic value generated trend")
        return None
    
    # Convert Decimal columns to float
    numeric_cols = ['electricity_sales', 'oil_revenues', 'other_revenues', 
                    'interest_income', 'share_in_net_income_of_associate', 'miscellaneous_income']
    data = data.astype({col: float for col in numeric_cols})
    
    # Convert to long format for line plot
    gen_long = pd.melt(data, id_vars=['year'], 
                      value_vars=numeric_cols,
                      var_name='revenue_type', value_name='value')
    
    # Create line plot
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.lineplot(x='year', y='value', hue='revenue_type', data=gen_long, linewidth=2.5, ax=ax)
    
    # Customize the plot
    plt.title('Economic Value Generated Components', fontsize=16)
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Value (in thousands)', fontsize=12)
    plt.legend(title='Revenue Type', fontsize=10)
    
    # Format y-axis with PHP symbol
    formatter = ticker.StrMethodFormatter('₱{x:,.0f}')
    ax.yaxis.set_major_formatter(formatter)
    
    # Set x-axis to have integer interval of 1
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    
    plt.tight_layout()
    return fig

def economic_value_retention_ratio():
    """
    Create bar chart with line showing economic value retention ratio over time.
    """
    # Get data from database calculations
    data = calculate_retention_ratio()
    
    if data is None or data.empty:
        print("No data available for retention ratio visualization")
        return None
    
    # Convert retention_ratio to float
    data['retention_ratio'] = data['retention_ratio'].astype(float)
    data['year'] = data['year'].astype(str)  # Convert year to string for x-axis
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Fix barplot to address FutureWarning
    sns.barplot(x='year', y='retention_ratio', hue='year', legend=False, data=data, palette='YlGnBu', ax=ax)
    
    # Add line connecting points - FIXED by adding data parameter
    sns.pointplot(data=data, x='year', y='retention_ratio', color='darkblue', markers='o', linestyles='-', ax=ax)
    
    # Add labels
    plt.title('Economic Value Retention Ratio', fontsize=16)
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Retention Ratio (Retained/Generated)', fontsize=12)
    
    # Format y-axis as percentage
    ax.yaxis.set_major_formatter(ticker.PercentFormatter(1.0))
    
    # Set x-axis to have integer interval of 1
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    
    # Add value labels - fixed to avoid TypeError with float conversion
    for i, v in enumerate(data['retention_ratio']):
        v_float = float(v)
        ax.text(i, v_float + 0.01, f"{v_float:.1%}", ha='center')
    
    plt.tight_layout()
    return fig

def economic_distributed_value_by_stakeholder_group(year=2023):
    """
    Create a horizontal bar chart showing distribution by stakeholder group.
    """
    # Get data from database
    data = query_stakeholder_distribution(year)
    
    if data is None or data.empty:
        print(f"No stakeholder distribution data available for year {year}")
        return None
    
    # Convert value to float
    data['value'] = data['value'].astype(float)
    
    # Sort by value in descending order
    data = data.sort_values('value', ascending=False)
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Fix barplot to address FutureWarning
    sns.barplot(x='value', y='stakeholder_group', hue='stakeholder_group', legend=False, data=data, palette='rocket', ax=ax)
    
    # Add labels
    plt.title(f'Economic Value Distribution by Stakeholder Group ({year})', fontsize=16)
    plt.xlabel('Value (in thousands)', fontsize=12)
    plt.ylabel('Stakeholder Group', fontsize=12)
    
    # Add value labels
    for i, v in enumerate(data['value']):
        v_float = float(v)
        ax.text(v_float + 1000, i, f"₱{v_float:,.0f}", va='center')
        
    # Format x-axis with commas and PHP symbol
    ax.xaxis.set_major_formatter(ticker.StrMethodFormatter('₱{x:,.0f}'))
    
    plt.tight_layout()
    return fig

def main():
    """
    Main function to demonstrate all visualizations.
    """
    # Set the style for all plots
    sns.set_theme(style="whitegrid")
    
    print("Generating visualizations from database data...")
    
    # Generate all visualizations
    fig1 = economic_value_flow_by_year()
    fig2 = economic_value_distribution_pie()
    fig3 = top_contributing_companies()
    fig4 = economic_value_generated_trend()
    fig5 = economic_value_retention_ratio()
    fig6 = economic_distributed_value_by_stakeholder_group()
    
    print("\nDisplaying visualizations...")
    
    # Display all figures
    plt.show()
    
    print("\nVisualization complete.")

if __name__ == "__main__":
    main() 