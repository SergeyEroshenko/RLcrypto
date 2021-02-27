import os
import pandas as pd
from glob import glob


class OrderBook:
    """
    Generator of Orderbooks data from pd.DataFrame
    """
    def __init__(self, data: pd.DataFrame):
        self.data = data

    def __iter__(self):
        self._item = 0
        self._times = self.data['timestamp'].unique()
        init_time = self._times[self._item]
        self._state = self.data[self.data['timestamp']==init_time]
        return self

    def __next__(self):
        if self._item < len(self._times):
            self._item += 1
            curr_time = self._times[self._item]
            curr_data = self.data[self.data['timestamp']==curr_time]
            self._state # TODO: Change state according curr_data
            return self._state
        else:
            raise(StopIteration)


DATA_PATH = os.path.join('.', 'data', 'sim_BTC-USDT')
files_list = glob(os.path.join(DATA_PATH, '*.csv'))

df = pd.read_csv(files_list[0], sep=';', index_col=0)
print(df.head())
orderbook = OrderBook(df)
state = iter(orderbook)
print(next(orderbook))