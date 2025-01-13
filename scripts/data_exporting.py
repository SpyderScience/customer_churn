import os

def save_to_csv(df, table_name, is_processed=False):
    # Define the folder path based on whether the data is raw or processed
    folder_path = 'data/processed_data' if is_processed else 'data/raw_data'
    
    # Create the folder if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # Define the complete file path
    file_path = os.path.join(folder_path, f'{table_name}.csv')
    
    # Check if the file already exists
    if not os.path.exists(file_path):
        # Save the DataFrame to CSV
        df.to_csv(file_path, index=False)
        print(f"File '{file_path}' has been saved successfully.")
    else:
        print(f"File '{file_path}' already exists. No action taken.")