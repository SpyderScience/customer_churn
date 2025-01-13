import pandas as pd
from db_connection import get_connection

def fetch_data_from_db(query):
    """
    Fetches data from the database and returns it as a pandas DataFrame.
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn)
