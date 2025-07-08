
import os
import pandas as pd
import seaborn as sns
from pandas import DataFrame
import matplotlib.pyplot as plt
from typing import List, Dict, Any

os.chdir(r"")

def analyse_column(series: pd.Series) -> Dict[str, Any]:
    """
    Analyse a single column - nulls, unique values, mode and modal count
    """
    most_common       = series.mode(dropna=True)
    most_common_val   = most_common[0] if not most_common.empty else None
    most_common_count = series.value_counts(dropna=True).iloc[0] if not most_common.empty else 0
    return {
        'num_nulls': series.isnull().sum(),
        'num_unique_values': series.nunique(dropna=True),
        'modal_value': most_common_val,
        'modal_n': most_common_count
    }

def plot_column_distribution(series: pd.Series, column_name: str):
    """
    Plot a single column's distribution
    """
    plt.figure(figsize=(10, 6))

    if series.dtype == 'object' or series.nunique(dropna=False) < 20:
        value_counts = series.value_counts(dropna=False)
        sns.barplot(x=value_counts.index.astype(str), y=value_counts.values)
        plt.xticks(rotation=45, ha='right')
        plt.title(f"Value Counts: {column_name}")
        plt.ylabel("Count")
        plt.xlabel(column_name)
    else:
        sns.histplot(series.dropna(), bins=30, kde=True)
        plt.title(f"Distribution: {column_name}")
        plt.xlabel(column_name)
        plt.ylabel("Frequency")

    plt.tight_layout()
    plt.show()
    plt.savefig(os.path.join(f"{column_name}_distribution.png"))
    plt.close()


def run_column_eda(df: str, output_dir: [str] = None,skip_columns: [List[str]] = None) -> [DataFrame]:
    """
    Orchestrates column summary and plotting
    """
    skip_columns = [col.upper() for col in (skip_columns or [])]
    summary = []

    for col in df.columns:
        col_stats = analyse_column(df[col])
        col_stats['column_name'] = col
        summary.append(col_stats)
        if col not in skip_columns:
            plot_column_distribution(df[col], col)
    return pd.DataFrame(summary)

#load data, print charts for each column (skipping some 'obvious' ones I don't want to view)
sales_df        = pd.read_csv('sample_data.csv', on_bad_lines='warn', encoding='latin1')
columns_to_skip = ['ORDERNUMBER','CONTACTLASTNAME','ORDERDATE','ORDERLINENUMBER','PHONE','CONTACTFIRSTNAME','CUSTOMERNAME','ADDRESSLINE1','ADDRESSLINE2','POSTCODE']
summary         = run_column_eda(sales_df, skip_columns=columns_to_skip)
summary.to_csv('summary.csv',index=False)

#%%
"""
  * Are there any factors that seem to predict or influence customer LTV?

"""

#get first and last order, calculate duration, total up sales and calculate an average
customer_lifetime = sales_df.groupby('CUSTOMERNAME').agg(
    first_order   = ('ORDERDATE', 'min'),
    last_order    = ('ORDERDATE', 'max'),
    months_active = ('MONTH_ID', 'nunique'),
    total_sales   = ('SALES', 'sum'),
).reset_index()
customer_lifetime['avg_sales_per_month'] = customer_lifetime['total_sales'] / customer_lifetime['months_active']
customer_lifetime = customer_lifetime.sort_values(by='total_sales', ascending=False)
customer_lifetime.to_csv('customer_lifetime.csv',index=False)

#get sale averages per territory
territory_summary = sales_df.groupby(['CUSTOMERNAME', 'TERRITORY'], dropna=False)['SALES'].sum().reset_index(name='total_sales')
territory_summary = territory_summary.groupby('TERRITORY', dropna=False)['total_sales'].agg(['mean', 'median', 'count']).reset_index()

#calculate CVI
customer_totals        = sales_df.groupby('CUSTOMERNAME')['SALES'].sum().reset_index(name='customer_total_sales')
customer_totals['CVI'] = customer_totals['customer_total_sales'] / (customer_totals['customer_total_sales'].mean())

#average CVI by territory
customer_territory            = sales_df[['CUSTOMERNAME', 'TERRITORY']].drop_duplicates()
customer_totals               = customer_totals.merge(customer_territory, on='CUSTOMERNAME', how='left')
cvi_by_territory              = customer_totals.groupby('TERRITORY', dropna=False)['CVI'].agg(['mean', 'median', 'count']).reset_index()
cvi_by_territory['TERRITORY'] = cvi_by_territory['TERRITORY'].fillna('Missing')
cvi_by_territory.to_csv('cvi_by_territory.csv',index=False)

#average CVI by product line
customer_product     = sales_df[['CUSTOMERNAME', 'PRODUCTLINE']].drop_duplicates()
customer_cvi_product = customer_totals.merge(customer_product, on='CUSTOMERNAME', how='left')
cvi_by_product       = customer_cvi_product.groupby('PRODUCTLINE')['CVI'].agg(['mean', 'median', 'count']).reset_index()
cvi_by_product.to_csv('cvi_by_product.csv',index=False)

