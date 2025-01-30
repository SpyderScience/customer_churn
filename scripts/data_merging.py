import pandas as pd
import os
def merge_data_for_analysis(df_DimCustomer, df_DimOrder, df_DimProduct, df_FactSales):
    """
    This function merges the dimension tables with the fact table to create a single dataset for analysis.
    It joins the fact table (df_FactSales) with dimension tables (df_DimCustomer, df_DimOrder, df_DimProduct)
    based on their respective keys.

    Args:
    - df_DimCustomer (pd.DataFrame): DataFrame for customer data
    - df_DimOrder (pd.DataFrame): DataFrame for order data
    - df_DimProduct (pd.DataFrame): DataFrame for product data
    - df_FactSales (pd.DataFrame): Fact table containing sales data

    Returns:
    - pd.DataFrame: A single DataFrame containing merged data for analysis
    """
    folder_path = 'data/raw_data/'
    if not os.path.exists(folder_path):
        print(f'{folder_path} - Path not found.')
        return 0
    df_DimCustomer = pd.read_csv(f'{folder_path}DimCustomer.csv')
    df_DimOrder = pd.read_csv(f'{folder_path}DimOrder.csv')
    df_DimProduct = pd.read_csv(f'{folder_path}DimProduct.csv')
    df_FactSales = pd.read_csv(f'{folder_path}FactSales.csv')

    # Merge with Dim_Order on 'order_id'
    merged_sales_orders = pd.merge(df_FactSales, df_DimOrder, on=['OrderKey'], how='left')

    # Step 2: Merge the result with Dim_Customer on CustomerId to get customer information
    merged_with_customer = pd.merge(merged_sales_orders, df_DimCustomer, on='CustomerKey', how='left')

    # Step 3: Merge the result with Dim_Product on ProductId to get product information
    final_merged_data = pd.merge(merged_with_customer, df_DimProduct, on='ProductKey', how='left')

    return final_merged_data
