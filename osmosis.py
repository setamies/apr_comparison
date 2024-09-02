import pandas as pd
import numpy as np
from functools import reduce
from utils import fetch_historical_quotes, create_df_from_coinmarketcap_data, convert_percent_columns
from config import TIME_START, TIME_END, INTERVAL, COINMARKETCAP_API_KEY, OSMOS_ID

def read_csv_with_date(file_path, column_names):
    df = pd.read_csv(file_path, skiprows=1, names=column_names)
    df['date'] = pd.to_datetime(df['date']).dt.normalize().dt.tz_localize('UTC')
    df = convert_percent_columns(df)
    return df

def fetch_coinmarketcap_data():
    osmo_data_json = fetch_historical_quotes(COINMARKETCAP_API_KEY, [OSMOS_ID], TIME_START, TIME_END, INTERVAL)
    osmo_data_df = create_df_from_coinmarketcap_data(osmo_data_json, OSMOS_ID)
    osmo_data_df['date'] = pd.to_datetime(osmo_data_df['date'], utc=True).dt.normalize()
    return osmo_data_df

def calculate_bonded_tokens(bonded_percent: np.ndarray, circulating_supply: np.ndarray) -> np.ndarray:
    """
    Calculate the number of bonded tokens based on bonded percentage and circulating supply.

    Args:
        bonded_percent (np.ndarray): Array of bonded percentages (as floats).
        circulating_supply (np.ndarray): Array of circulating supply values.

    Returns:
        np.ndarray: Array of bonded tokens (as integers), with NaN for missing data.
    """
    # Make sure calculations are only done when both arrays contain values
    valid_mask = ~np.isnan(bonded_percent) & ~np.isnan(circulating_supply)
    result = np.full(bonded_percent.shape, np.nan)
    result[valid_mask] = np.round(bonded_percent[valid_mask] * circulating_supply[valid_mask]).astype(int)
    
    return result
    

def merge_osmosis_data():
    bonded_percentage = read_csv_with_date('osmosis/osmosis_bonded_percentage.csv', ['date', 'bonded_percent'])
    staking_apr = read_csv_with_date('osmosis/osmosis_staking_apr.csv', ['date', 'apr'])
    circulating_supply_and_price = fetch_coinmarketcap_data()

    dfs = [bonded_percentage, staking_apr, circulating_supply_and_price]

    merged_df = reduce(lambda left, right: pd.merge(left, right, on='date', how='outer'), dfs)
    merged_df = merged_df.sort_values('date')
    merged_df['date'] = merged_df['date'].dt.tz_localize(None)
    merged_df['bonded_tokens'] = calculate_bonded_tokens(np.array(merged_df['bonded_percent']), np.array(merged_df['circulating_supply']))
    merged_df['has_liquid_staking'] = True

    return merged_df

if __name__ == "__main__":
    merged_data = merge_osmosis_data()
    merged_data.to_csv('osmosis/osmosis_data.csv', index=False)
    print("Data merged and saved to osmosis/osmosis_data.csv")
