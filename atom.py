import pandas as pd
from functools import reduce
from utils import fetch_historical_quotes, create_df_from_coinmarketcap_data, store_data_in_csv, convert_percent_columns

from config import TIME_START, TIME_END, INTERVAL, COINMARKETCAP_API_KEY, ATOM_ID

def read_csv_with_date(file_path, column_names):
    df = pd.read_csv(file_path, skiprows=1, names=column_names)
    df['date'] = pd.to_datetime(df['date']).dt.normalize().dt.tz_localize('UTC')
    df = convert_percent_columns(df)
    return df

def fetch_coinmarketcap_data():
    atom_data_json = fetch_historical_quotes(COINMARKETCAP_API_KEY, [ATOM_ID], TIME_START, TIME_END, INTERVAL)
    atom_data_df = create_df_from_coinmarketcap_data(atom_data_json, ATOM_ID)
    atom_data_df['date'] = pd.to_datetime(atom_data_df['date'], utc=True).dt.normalize()
    return atom_data_df

def merge_atom_data():
    bonded_tokens = read_csv_with_date('atom/atom_bonded_tokens.csv', ['date', 'bonded_supply'])
    bonded_tokens['bonded_supply'] = bonded_tokens['bonded_supply'].str.replace(',', '').astype(int)
    inflation = read_csv_with_date('atom/atom_inflation.csv', ['date', 'inflation'])
    apr = read_csv_with_date('atom/atom_staking_apr.csv', ['date', 'staking_apr'])
    bonded_percent = read_csv_with_date('atom/atom_bonded_percent.csv', ['date', 'bonded_percent'])
    circulating_supply_and_price = fetch_coinmarketcap_data()
    
    data_frames = [bonded_tokens, inflation, apr, bonded_percent, circulating_supply_and_price]
    df_merged = reduce(lambda left, right: pd.merge(left, right, on='date', how='outer'), data_frames)
    
    df_merged = df_merged.sort_values('date')
    df_merged['date'] = df_merged['date'].dt.tz_localize(None)
    df_merged['has_liquid_staking'] = True

    return df_merged

if __name__ == "__main__":
    merged_data = merge_atom_data()
    store_data_in_csv(merged_data, 'atom_data.csv')
    print("Data merged and saved to atom/atom_data.csv")