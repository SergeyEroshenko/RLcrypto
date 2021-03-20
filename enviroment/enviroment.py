import os
import numpy as np
import pandas as pd
from glob import glob


class OrderBook:
    """
    Generator of Orderbooks data from pd.DataFrame
    """
    def __init__(self, data: pd.DataFrame):
        self.data = data.set_index('price')

    def __iter__(self):
        self._item = 0
        self._times = self.data['timestamp'].unique()
        print(len(self._times))
        init_time = self._times[self._item]
        self._state = self.data[self.data['timestamp']==init_time]
        return self

    def __next__(self):
        if self._item < len(self._times) - 1:
            self._item += 1
            curr_time = self._times[self._item]
            curr_data = self.data[self.data['timestamp']==curr_time]
            self._update_state(curr_data)
            return self._state
        else:
            raise(StopIteration)
    
    def _update_state(self, data):
        curr_prices = self._state.index
        del_prices = data[data['size']==0].index
        change_prices = data[
            (data.index.isin(curr_prices)) 
            & ~(data.index.isin(del_prices))].index
        add_prices = data[
            ~(data.index.isin(curr_prices)) 
            & ~(data.index.isin(del_prices))].index

        self._state = self._state.drop(del_prices)
        self._state.loc[change_prices] = data.loc[change_prices]
        self._state = self._state.append(data.loc[add_prices])
        self._state = self._state.sort_index()


DATA_PATH = os.path.join('.', 'data', 'sim_BTC-USDT')
files_list = glob(os.path.join(DATA_PATH, '*.csv'))

df = pd.read_csv(files_list[0], sep=';', index_col=0)


xx_price = 58111.3
orderbook = OrderBook(df)
for idx, item in enumerate(orderbook):
    if xx_price in item.index:
        print(item.loc[xx_price])
# state = iter(orderbook)
# print(next(orderbook))