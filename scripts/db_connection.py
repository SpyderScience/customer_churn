import pyodbc
import pandas as pd
import sys
import os
# Add the config directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config')))

import config  # Import configuration file for DB credentials

class DatabaseConnection:
    """
    This class manages the connection to the database.
    """
    def __init__(self):
        self.conn = None

    def connect(self):
        """
        Establish a connection to the database using credentials from the config file.
        """
        try:
            conn_str = (
                f'DRIVER={{{config.DB_CONFIG["driver"]}}};'
                f'SERVER={config.DB_CONFIG["server"]};'
                f'DATABASE={config.DB_CONFIG["database"]};'
                f'UID={config.DB_CONFIG["username"]};'
                f'PWD={config.DB_CONFIG["password"]}'
            )
            self.conn = pyodbc.connect(conn_str)
            print("Connection successful")
        except Exception as e:
            print(f"Error occurred while connecting to the database: {e}")

    def fetch_data(self, query):
        """
        Fetch data from the database and return it as a DataFrame.
        :param query: SQL query to fetch data
        :return: DataFrame containing query results
        """
        if self.conn:
            try:
                return pd.read_sql(query, self.conn)
            except Exception as e:
                print(f"Error occurred while fetching data: {e}")
                return None
        else:
            print("No connection established. Please connect first.")
            return None

    def close_connection(self):
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()
            print("Connection closed.")
        else:
            print("No active connection to close.")
