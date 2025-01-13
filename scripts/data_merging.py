import pandas as pd

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
    # Merge Fact_Sales with Dim_Customer on 'customer_id'
    merged_data = pd.merge(df_FactSales, df_DimCustomer, on='customer_id', how='left')

    # Merge with Dim_Order on 'order_id'
    merged_data = pd.merge(merged_data, df_DimOrder, on='order_id', how='left')

    # Merge with Dim_Product on 'product_id'
    merged_data = pd.merge(merged_data, df_DimProduct, on='product_id', how='left')

    return merged_data
