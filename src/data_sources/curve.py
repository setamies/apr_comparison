import requests
import pandas as pd
from dune_client.client import DuneClient
import matplotlib.pyplot as plt
from utils import convert_and_format_timestamp
from config import DUNE_API_KEY  

def main():
    dune = DuneClient(DUNE_API_KEY)
    merged_df = merge_crv_data()
    print(merged_df)

def save_dune_query_to_csv(dune, query_id, filename):
    query_result = dune.get_latest_result(query_id)
    data_rows = query_result.result.rows
    df = pd.DataFrame(data_rows)
    df.to_csv(filename, index=False)

def fetch_crv_prices(dune):
    save_dune_query_to_csv(dune, 3994146, 'data/crv/daily_price_data.csv')

def fetch_crv_supply(dune):
    save_dune_query_to_csv(dune, 3994271, 'data/crv/supply_data.csv')

def fetch_crv_misc(dune):
    save_dune_query_to_csv(dune, 3893488, 'data/crv/misc_data.csv')

def fetch_crv_apy(dune):
    save_dune_query_to_csv(dune, 3994290, 'data/crv/apy_data.csv')

def merge_crv_data():
    price_df = pd.read_csv('data/crv/daily_price_data.csv').drop_duplicates()
    supply_df = pd.read_csv('data/crv/supply_data.csv').drop_duplicates()
    apy_df = pd.read_csv('data/crv/apy_data.csv').drop_duplicates()

    price_df = price_df.rename(columns={'day': 'timestamp', 'price': 'price'})
    supply_df = supply_df.rename(columns={
        'CRV': 'circ_supply',
        'date': 'timestamp',
        'veCRV': 'bonded_supply',
        'veCRV_Percent': 'bonded_percent'
    })
    supply_df['total_supply'] = supply_df['circ_supply'] + supply_df['bonded_supply']
    supply_df['bonded_percent'] = supply_df['bonded_percent'] / 100
    supply_df['daily_inflation_rate'] = supply_df['total_supply'].pct_change()
    apy_df = apy_df.rename(columns={'day': 'timestamp', 'daily_apy': 'apr'})
    apy_df['apr'] = (apy_df['apr'] / 1200) * 365

    price_df = convert_and_format_timestamp(price_df, 'timestamp')
    supply_df = convert_and_format_timestamp(supply_df, 'timestamp')
    apy_df = convert_and_format_timestamp(apy_df, 'timestamp')

    merged_df = price_df.merge(supply_df, on='timestamp', how='outer') \
                        .merge(apy_df, on='timestamp', how='outer')

    merged_df = merged_df.sort_values(by='timestamp').reset_index(drop=True)
    merged_df.set_index('timestamp', inplace=True)
    merged_df['has_liquid_staking'] = False

    return merged_df.dropna()

if __name__ == "__main__":
    main()