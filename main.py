import pandas as pd
from typing import Dict, List
from osmosis import merge_osmosis_data
from atom import merge_atom_data
from dydx import merge_dydx_data
from curve import merge_crv_data
from gmx import merge_gmx_data
from balancer import merge_bal_data

RENAME_DICT: Dict[str, str] = {
    'bonded_percent': 'bonded_percentage',
    'staking_apr': 'apr',
    'circulating_supply': 'circ_supply',
    'bonded_supply': 'bonded_tokens',
    'total_tokens': 'bonded_tokens',
    'percentage_bonded': 'bonded_percentage',
    'timestamp': 'date',  
    'daily_inflation_rate': 'inflation'
}

def standardize_columns(df: pd.DataFrame, rename_dict: Dict[str, str]) -> pd.DataFrame:
    """Standardize column names in the DataFrame."""
    return df.rename(columns=rename_dict)

def standardize_date(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize date column to daily accuracy and ensure it's not an index."""
    if df.index.name == 'timestamp':
        df = df.reset_index()
    
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], utc=True).dt.date
    elif 'timestamp' in df.columns:
        df['date'] = pd.to_datetime(df['timestamp'], utc=True).dt.date
        df = df.drop(columns=['timestamp'])
    
    return df

def merge_all_data() -> pd.DataFrame:
    """
    Merge data from different chains into a single DataFrame.
    
    Returns:
        pandas.DataFrame: Merged DataFrame containing data from all chains.
    """
    data_sources = {
        'Osmosis': merge_osmosis_data,
        'Atom': merge_atom_data,
        'dYdX': merge_dydx_data,
        'Curve': merge_crv_data,
        'GMX': merge_gmx_data,
        'Balancer': merge_bal_data
    }
    all_data = []

    for chain, merge_func in data_sources.items():
        df = merge_func()
        df = standardize_columns(df, RENAME_DICT)
        df = standardize_date(df)
        df['chain'] = chain
        all_data.append(df)
        
    merged_data = pd.concat(all_data, ignore_index=True)
    merged_data = merged_data.sort_values(['chain', 'date'])

    return merged_data

if __name__ == "__main__":
    dataframes = {
        'Osmosis': merge_osmosis_data(),
        'Atom': merge_atom_data(),
        'dYdX': merge_dydx_data(),
        'Curve': merge_crv_data(),
        'GMX': merge_gmx_data(),
        'Balancer': merge_bal_data()
    }

    merged_data = merge_all_data()
    merged_data.to_csv('all_chains_data.csv', index=False)
    
    print("\nMerged data saved to 'all_chains_data.csv'")
    print("\nDescriptive statistics of merged data:")
    print(merged_data.describe(include='all'))
    print("\nMissing values in merged data:")
    print(merged_data.isnull().sum())
    print("\nData types of merged data columns:")
    print(merged_data.dtypes)