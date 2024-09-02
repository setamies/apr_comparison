import os
from google.cloud import bigquery
import pandas as pd
import requests
from typing import Dict

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bigquery_service.json"

def fetch_validator_data(chain_id: str, table:str) -> pd.DataFrame:
    """
    Fetch validator data from BigQuery for a specific chain and table.

    Args:
        chain_id (str): The ID of the blockchain chain (e.g., 'dydx_mainnet').
        table (str): The table name to query (e.g., 'dydx_validators').

    Returns:
        pd.DataFrame: A DataFrame containing the query results.
    """
    # Initialize a BigQuery client
    client = bigquery.Client()

    # Define the query with dynamic chain_id and table
    query = f"""
WITH Daily_Tokens AS (
    SELECT 
        DATE(ingestion_timestamp) AS date, 
        SUM(CAST(tokens AS NUMERIC)) AS total_tokens  -- Sum the tokens for each day
    FROM 
        `numia-data.{chain_id}.{table}` 
    WHERE 
        status = 'BOND_STATUS_BONDED' 
        AND DATE(ingestion_timestamp) BETWEEN '2023-01-01' AND CURRENT_DATE()
    GROUP BY 
        DATE(ingestion_timestamp)  -- Group by the date to get total tokens per day
)

SELECT 
    date,
    total_tokens
FROM 
    Daily_Tokens
ORDER BY 
    date DESC;
    """

    # Execute the query
    query_job = client.query(query)
    results = query_job.result()

    # Convert the results to a DataFrame
    df = results.to_dataframe()
    df['total_tokens'] = convert_1e18_column_to_float(df['total_tokens'])
    return df

def convert_1e18_column_to_float(series: pd.Series) -> pd.Series:
    """
    Convert a column in a DataFrame from a scaled integer (scaled by 1e18) to a normal float.

    This function assumes the data is scaled by 1e18 (common in blockchain datasets) and converts it to a float by dividing by 1e18.

    Args:
        series (pd.Series): The column to convert, typically holding large integers scaled by 1e18.

    Returns:
        pd.Series: The column with values converted to floating-point numbers, representing the actual values.
    """
    # Convert the series to numeric, coercing errors which will turn non-convertible values into NaN
    numeric_series = pd.to_numeric(series, errors='coerce')
    
    # Divide by 1e18 to scale down to a float
    return numeric_series / 1e18

def fetch_historical_quotes(api_key: str, ids: list, time_start: str, time_end: str, interval: str):
    """ Fetch historical market quotes for a cryptocurrency id(s). Supports multiple IDs simultaneously."""
    ids = ','.join(ids)
    url = "https://pro-api.coinmarketcap.com/v3/cryptocurrency/quotes/historical"
    headers = {'X-CMC_PRO_API_KEY': api_key}
    params = {
        'id': ids,
        'time_start': time_start,
        'time_end': time_end,
        'interval': interval,
        'convert': 'USD',
        'aux': 'price,volume,market_cap,circulating_supply,total_supply,quote_timestamp,is_active,is_fiat'
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        print(response.text)
        return {}

def create_df_from_coinmarketcap_data(data: Dict, id: str) -> pd.DataFrame:
    """
    Create a DataFrame from CoinMarketCap data for a specified token ID.

    Args:
        data (Dict): CoinMarketCap data.
        id (str): Token ID.

    Returns:
        pd.DataFrame: DataFrame of token's data.
    """
    df = pd.json_normalize(data['data'][id]['quotes'])
    df['token'] = data['data'][id]['name']
    
    df = clean_column_names(df)
    df = df.loc[:, ~df.columns.duplicated()]
    df.rename(columns={'timestamp': 'date'}, inplace=True)
    return df

def store_data_in_csv(df: pd.DataFrame, filename: str) -> None:
    # Extract the base name before '_data.csv'
    folder_name = filename.split('_data.csv')[0]
    print("Folder to be created/used:", folder_name)

    # Ensure the folder exists
    os.makedirs(folder_name, exist_ok=True)

    # Define the path to store the CSV file, ensuring the file name is correctly formatted
    file_path = os.path.join(folder_name, f'{folder_name}_data.csv')
    print("Storing data in:", file_path)

    # Store the DataFrame in CSV format without an index
    df.to_csv(file_path, index=False)

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove the 'quote.USD.' prefix from column names in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to clean.

    Returns:
        pd.DataFrame: DataFrame with cleaned column names.
    """
    df.columns = df.columns.str.replace('quote.USD.', '', regex=False)
    return df

def convert_percent_to_float(percent_str):
    """
    Convert a percentage string to a float value.

    Args:
        percent_str (str): Percentage as a string (e.g., '46.1%').

    Returns:
        float: Percentage as a float (e.g., 0.461).
    """
    return float(percent_str.strip('%')) / 100

def convert_percent_columns(df):
    """
    Convert any columns containing percentage strings to float values.

    Args:
        df (pd.DataFrame): DataFrame to process.

    Returns:
        pd.DataFrame: DataFrame with percentage columns converted to floats.
    """
    for column in df.columns:
        if df[column].dtype == 'object' and df[column].str.contains('%').any():
            df[column] = df[column].apply(convert_percent_to_float)
    return df

def convert_and_format_timestamp(df, column_name):
    df[column_name] = pd.to_datetime(df[column_name], utc=True)
    df[column_name] = df[column_name].dt.strftime('%Y-%m-%d')
    return df
