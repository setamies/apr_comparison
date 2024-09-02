import json
from typing import Dict, List

import pandas as pd
import zlib

from utils import fetch_validator_data, fetch_historical_quotes, create_df_from_coinmarketcap_data, clean_column_names
from config import TIME_START, TIME_END, INTERVAL, COINMARKETCAP_API_KEY, DYDX_NATIVE_ID, DYDX_ETH_ID, CUTOFF_DATE

def decompress_data(file_path: str) -> str:
    """
    Decompress data from a file.

    Args:
        file_path (str): Path to the compressed file.

    Returns:
        str: Decompressed data as a string.
    """
    with open(file_path, 'rb') as f:
        decompressed_data = zlib.decompress(f.read())
    return decompressed_data.decode('utf-8') if isinstance(decompressed_data, bytes) else decompressed_data

def convert_json_to_dataframe(json_data: str) -> pd.DataFrame:
    """
    Convert JSON data to a pandas DataFrame.

    Args:
        json_data (str): JSON data as a string.

    Returns:
        pd.DataFrame: DataFrame created from the JSON data.
    """
    data = json.loads(json_data)
    return pd.DataFrame(data['data'])

def save_dataframe_to_csv(df: pd.DataFrame, columns: List[str], file_path: str) -> None:
    """
    Save selected columns of a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        columns (List[str]): List of column names to save.
        file_path (str): Path to save the CSV file.
    """
    df[columns].to_csv(file_path, index=False)

def create_df_from_coinmarketcap_data(data: Dict, id_list: List[str]) -> pd.DataFrame:
    """
    Create a DataFrame from CoinMarketCap data for specified token IDs.

    Args:
        data (Dict): CoinMarketCap data.
        id_list (List[str]): List of token IDs.

    Returns:
        pd.DataFrame: Combined DataFrame of all tokens' data.
    """
    all_dataframes = []
    
    for id_number in id_list:
        print(f"Processing ID: {id_number}")
        df = pd.json_normalize(data['data'][id_number]['quotes'])
        df['token'] = data['data'][id_number]['name']
        all_dataframes.append(df)
    
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    combined_df = clean_column_names(combined_df)
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
    combined_df.rename(columns={'timestamp': 'date'}, inplace=True)
    return combined_df

def filter_and_combine_data(df: pd.DataFrame, cutoff_date: str) -> pd.DataFrame:
    """
    Filter and combine data for dYdX (Native) and dYdX (ethDYDX) based on a cutoff date.

    Args:
        df (pd.DataFrame): DataFrame containing all dYdX data.
        cutoff_date (str): Date to use as a cutoff for filtering.

    Returns:
        pd.DataFrame: Filtered and combined DataFrame.
    """
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)

    native_df = df[(df['token'] == 'dYdX (Native)') & (df['date'] >= cutoff_date)]
    eth_dydx_df = df[(df['token'] == 'dYdX (ethDYDX)') & (df['date'] < cutoff_date)]

    final_df = pd.concat([native_df, eth_dydx_df], ignore_index=True)
    final_df.sort_values(by='date', inplace=True)
    final_df.reset_index(inplace=True)
    
    return final_df

def merge_dydx_data() -> pd.DataFrame:
    """
    Main function to process dYdX data.

    Returns:
        pd.DataFrame: Processed dYdX token circulation data.
    """
    dydx_bonded_tokens_df = fetch_validator_data('dydx_mainnet', 'dydx_validators')
    dydx_bonded_tokens_df['date'] = pd.to_datetime(dydx_bonded_tokens_df['date'])

    file_path = 'dydx/fully[2024-06-19--f1112].dat'
    decompressed_data = decompress_data(file_path)
    dydx_apr_df = convert_json_to_dataframe(decompressed_data)
    dydx_apr_df['date'] = pd.to_datetime(dydx_apr_df['date'])
    save_dataframe_to_csv(dydx_apr_df, ['date', 'apr'], 'dydx/dydx_apr.csv')

    id_dydx = [DYDX_NATIVE_ID, DYDX_ETH_ID]
    dydx_circulating_supply = fetch_historical_quotes(COINMARKETCAP_API_KEY, id_dydx, TIME_START, TIME_END, INTERVAL)
    combined_data = create_df_from_coinmarketcap_data(dydx_circulating_supply, id_dydx)
    dydx_token_circulation_df = filter_and_combine_data(combined_data, CUTOFF_DATE)
    dydx_token_circulation_df['date'] = pd.to_datetime(dydx_token_circulation_df['date']).dt.tz_localize(None)
    dydx_token_circulation_df = dydx_token_circulation_df.merge(dydx_bonded_tokens_df, on='date', how='left')
    dydx_token_circulation_df['percentage_bonded'] = dydx_token_circulation_df['total_tokens'] / dydx_token_circulation_df['circulating_supply']
    dydx_token_circulation_df.to_csv('dydx/dydx_token_circulation.csv', index=False)
    dydx_token_circulation_df = dydx_token_circulation_df.merge(dydx_apr_df[['date', 'apr']], on='date', how='left')
    dydx_token_circulation_df.drop(columns=['index'], inplace=True)
    dydx_token_circulation_df['has_liquid_staking'] = True

    return dydx_token_circulation_df

if __name__ == "__main__":
    result_df = merge_dydx_data()
    print(result_df.head())
