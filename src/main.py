from scripts.data_fetching import fetch_data_from_db
from scripts.data_exporting import save_to_csv
from scripts.data_merging import merge_data_for_analysis

def main():
    queries = {
        'DimCustomer': "SELECT * FROM Dim_Customer",
        'DimOrder': "SELECT * FROM Dim_Order",
        'DimProduct': "SELECT * FROM Dim_Product",
        'FactSales': "SELECT * FROM Fact_Sales"
    }

    for table_name, query in queries.items():
        df = fetch_data_from_db(query)
        save_to_csv(df, f'{table_name}.csv', is_processed=False)

    merged_data = merge_data_for_analysis(query.items()[0],query.items()[1],query.items()[2],query.items()[3])
    save_to_csv(df,f'{merged_data}.csv', is_processed=True)

if __name__ == "__main__":
    main()
