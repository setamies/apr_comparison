from dotenv import load_dotenv
import os

load_dotenv()

COINMARKETCAP_API_KEY = os.getenv("COINMARKETCAP_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")

# Specifications for the coinmarketcap API request
TIME_START = '2023-09-02T00:00:00Z'
TIME_END = '2024-08-29T23:59:59Z'
INTERVAL = 'daily'
CUTOFF_DATE = '2023-12-12'

OSMOS_ID = '12220'
ATOM_ID = '3794'
DYDX_NATIVE_ID = '28324'
DYDX_ETH_ID = '11156'

