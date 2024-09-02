import requests
import pandas as pd
from dune_client.client import DuneClient
import matplotlib.pyplot as plt
from utils import convert_and_format_timestamp
from config import DUNE_API_KEY  

def main():
    # Initialize the Dune client with your API key
    dune = DuneClient(DUNE_API_KEY)
    merged_df = merge_crv_data()
    print(merged_df)

def save_dune_query_to_csv(dune, query_id, filename):
    query_result = dune.get_latest_result(query_id)
    data_rows = query_result.result.rows
    df = pd.DataFrame(data_rows)
    df.to_csv(filename, index=False)

def fetch_crv_prices(dune):
    save_dune_query_to_csv(dune, 3994146, 'crv/daily_price_data.csv')

def fetch_crv_supply(dune):
    save_dune_query_to_csv(dune, 3994271, 'crv/supply_data.csv')

def fetch_crv_misc(dune):
    save_dune_query_to_csv(dune, 3893488, 'crv/misc_data.csv')

def fetch_crv_apy(dune):
    save_dune_query_to_csv(dune, 3994290, 'crv/apy_data.csv')

def merge_crv_data():
    '''
    Returns a dataframe with the Curve (CRV) data.
    Note:
    - Bonded CRV is known as veCRV
    '''
    # Load the data from CSV files
    price_df = pd.read_csv('crv/daily_price_data.csv').drop_duplicates()
    supply_df = pd.read_csv('crv/supply_data.csv').drop_duplicates()
    apy_df = pd.read_csv('crv/apy_data.csv').drop_duplicates()

    # Rename columns as per the previous discussion
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
    # This is because the original was multiplied by 100 * 12. 100 for dec -> pct and 12 for monthly -> yearly. We are gong daily -> yearly.
    apy_df['apr'] = (apy_df['apr'] / 1200) * 365

    # Convert timestamp columns to datetime
    price_df = convert_and_format_timestamp(price_df, 'timestamp')
    supply_df = convert_and_format_timestamp(supply_df, 'timestamp')
    apy_df = convert_and_format_timestamp(apy_df, 'timestamp')

    # Merge the dataframes on the 'timestamp' column
    merged_df = price_df.merge(supply_df, on='timestamp', how='outer') \
                        .merge(apy_df, on='timestamp', how='outer')

    # Sort the merged dataframe by timestamp
    merged_df = merged_df.sort_values(by='timestamp').reset_index(drop=True)

    # Set the index to the timestamp column
    merged_df.set_index('timestamp', inplace=True)
    merged_df['has_liquid_staking'] = False

    # Return the merged dataframe
    return merged_df.dropna()


if __name__ == "__main__":
    main()
