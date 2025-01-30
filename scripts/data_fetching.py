import pandas as pd
from scripts.db_connection import DatabaseConnection  # Import DatabaseConnection class

def fetch_data_from_db(query):
    """
    Fetches data from the database and returns it as a pandas DataFrame.
    """
    db = DatabaseConnection()  # Create an instance of DatabaseConnection
    db.connect()  # Connect to the database
    data = db.fetch_data(query)  # Fetch data using the provided query
    db.close_connection()  # Close the connection
    return data
