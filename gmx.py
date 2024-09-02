import requests
import pandas as pd
from dune_client.client import DuneClient
import matplotlib.pyplot as plt
from config import DUNE_API_KEY  
from utils import convert_and_format_timestamp  

def main():
    # Initialize the Dune client with your API key
    dune = DuneClient(DUNE_API_KEY)
    merged_df = merge_gmx_data()
    print(merged_df)

def save_dune_query_to_csv(dune, query_id, filename):
    query_result = dune.get_latest_result(query_id)
    data_rows = query_result.result.rows
    df = pd.DataFrame(data_rows)
    df.to_csv(filename, index=False)

def fetch_glp_data(dune):
    save_dune_query_to_csv(dune, 1066775, 'gmx/glp_data.csv')

def fetch_gmx_supply(dune):
    save_dune_query_to_csv(dune, 1108993, 'gmx/supply_data.csv')

def fetch_gmx_price(dune):
    save_dune_query_to_csv(dune, 3997647, 'gmx/price_data.csv')

def fetch_gmx_staking(dune):
    save_dune_query_to_csv(dune, 1036839, 'gmx/staking_data.csv')

def fetch_gmx_apy(dune):
    save_dune_query_to_csv(dune, 2657814, 'gmx/apy_data.csv')

def merge_gmx_data():
    supply_df = pd.read_csv('gmx/supply_data.csv')
    price_df = pd.read_csv('gmx/price_data.csv')
    staking_df = pd.read_csv('gmx/staking_data.csv')
    apy_df = pd.read_csv('gmx/apy_data.csv')

    staking_df['bonded_supply'] = staking_df['gmx_s_total'] - staking_df['gmx_u_total']

    supply_df = supply_df.rename(columns={'time': 'timestamp', 'total_supply': 'max_supply', 'cir_supply': 'total_supply'})
    price_df = price_df.rename(columns={'time': 'timestamp', 'gmx_price': 'price'})
    staking_df = staking_df.rename(columns={'time_scale': 'timestamp'})
    apy_df = apy_df.rename(columns={'day': 'timestamp', 'gmx_apr': 'apr'})

    supply_df = supply_df[['timestamp', 'total_supply']]
    price_df = price_df[['timestamp', 'price']]
    staking_df = staking_df[['timestamp', 'bonded_supply']]
    apy_df = apy_df[['timestamp', 'apr']]

    price_df = convert_and_format_timestamp(price_df, 'timestamp')
    supply_df = convert_and_format_timestamp(supply_df, 'timestamp')
    staking_df = convert_and_format_timestamp(staking_df, 'timestamp')
    apy_df = convert_and_format_timestamp(apy_df, 'timestamp')

    merged_df = price_df.merge(supply_df, on='timestamp', how='outer') \
                        .merge(staking_df, on='timestamp', how='outer') \
                        .merge(apy_df, on='timestamp', how='outer')
    merged_df = merged_df.sort_values(by='timestamp', ascending=True)

    merged_df['circ_supply'] = merged_df['total_supply'] - merged_df['bonded_supply']
    merged_df['bonded_percent'] = merged_df['bonded_supply'] / merged_df['total_supply']
    merged_df['daily_inflation_rate'] = merged_df['circ_supply'].pct_change()

    merged_df.set_index('timestamp', inplace=True)
    merged_df['has_liquid_staking'] = False

    return merged_df.dropna()

if __name__ == "__main__":
    main()
