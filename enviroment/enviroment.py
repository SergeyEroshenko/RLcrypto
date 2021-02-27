import os
import pandas as pd
from glob import glob


class OrderBook:

    pass


DATA_PATH = os.path.join('.', 'data', 'sim_BTC-USDT')
files_list = glob(os.path.join(DATA_PATH, '*.csv'))

df = pd.read_csv(files_list[0], sep=';', index_col=0)
print(df.head())