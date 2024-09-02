import os
from google.cloud import bigquery
import pandas as pd
import requests
from typing import Dict
from config import GOOGLE_APPLICATION_CREDENTIALS

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

def fetch_validator_data(chain_id: str, table: str) -> pd.DataFrame:
    """
    Fetch validator data from BigQuery for a specific chain and table.
    """
    client = bigquery.Client()
    query = f"""
    WITH Daily_Tokens AS (
        SELECT 
            DATE(ingestion_timestamp) AS date, 
            SUM(CAST(tokens AS NUMERIC)) AS total_tokens
        FROM 
            `numia-data.{chain_id}.{table}` 
        WHERE 
            status = 'BOND_STATUS_BONDED' 
            AND DATE(ingestion_timestamp) BETWEEN '2023-01-01' AND CURRENT_DATE()
        GROUP BY 
            DATE(ingestion_timestamp)
    )
    SELECT 
        date,
        total_tokens
    FROM 
        Daily_Tokens
    ORDER BY 
        date DESC;
    """
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    df['total_tokens'] = convert_1e18_column_to_float(df['total_tokens'])
    return df

def convert_1e18_column_to_float(series: pd.Series) -> pd.Series:
    numeric_series = pd.to_numeric(series, errors='coerce')
    return numeric_series / 1e18

def fetch_historical_quotes(api_key: str, ids: list, time_start: str, time_end: str, interval: str):
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
    df = pd.json_normalize(data['data'][id]['quotes'])
    df['token'] = data['data'][id]['name']
    df = clean_column_names(df)
    df = df.loc[:, ~df.columns.duplicated()]
    df.rename(columns={'timestamp': 'date'}, inplace=True)
    return df

def store_data_in_csv(df: pd.DataFrame, filename: str) -> None:
    folder_name = filename.split('_data.csv')[0]
    os.makedirs(folder_name, exist_ok=True)
    file_path = os.path.join(folder_name, f'{folder_name}_data.csv')
    df.to_csv(file_path, index=False)

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.replace('quote.USD.', '', regex=False)
    return df

def convert_percent_to_float(percent_str):
    return float(percent_str.strip('%')) / 100

def convert_percent_columns(df):
    for column in df.columns:
        if df[column].dtype == 'object' and df[column].str.contains('%').any():
            df[column] = df[column].apply(convert_percent_to_float)
    return df

def convert_and_format_timestamp(df, column_name):
    df[column_name] = pd.to_datetime(df[column_name], utc=True)
    df[column_name] = df[column_name].dt.strftime('%Y-%m-%d')
    return df