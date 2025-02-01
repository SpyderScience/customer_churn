from scripts.data_fetching import fetch_data_from_db
from scripts.data_exporting import save_to_csv
from scripts.data_merging import merge_data_for_analysis

def main():
    queries = {
        'DimCustomer': "SELECT * FROM Dim_Customer",
        'DimOrder': "SELECT * FROM Dim_Order",
        'DimProduct': "SELECT * FROM Dim_Product",
        'FactSales': "SELECT * FROM Fact_Sales"
        #'DimOrder_FactSales': "SELECT fs.SalesKey, fs.OrderKey, fs.ProductKey, fs.CustomerKey, fs.CustomerType, fs.Quantity AS SalesQuantity, fs.TotalAmount, fs.OrderDate, fs.OrderYear, fs.OrderMonth, fs.OrderDay, fs.IsHighValueOrder, do.OrderId, do.StoreId, do.CustomerId, do.CustomerIp, do.OrderStatusId, do.CreatedOnUtc, do.OrderSubTotalExclTax, do.OrderSubTotalInclTax, do.OrderTotal, do.OrderItemId, do.ProductId, do.Quantity AS OrderQuantity, do.UnitPriceInclTax, do.UnitPriceExclTax, do.IsPickupInstore FROM FactSales fs LEFT JOIN DimOrder do ON fs.OrderKey = do.OrderKey;"
    }

    for table_name, query in queries.items():
        df = fetch_data_from_db(query)
        save_to_csv(df, f'{table_name}.csv', is_processed=False)
    query_ls = list(queries.keys())
    merged_data = merge_data_for_analysis(query_ls[0],query_ls[1],query_ls[2],query_ls[3])
    save_to_csv(merged_data,'merged_data.csv', is_processed=True)

if __name__ == "__main__":
    main()
