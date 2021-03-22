import os
import numpy as np
import pandas as pd
from glob import glob


class OrderBookController:
    """
    Generator of Orderbooks data from pd.DataFrame
    """
    def __init__(self, data: pd.DataFrame):
        self.data = data.set_index('price')
        self.best_ask = None
        self.best_bid = None

    def __iter__(self):
        self._item = 0
        self._times = sorted(self.data['timestamp'].unique())
        init_time = self._times[self._item]
        self._state = self.data[self.data['timestamp']==init_time]
        self._update_best_prices()
        return self

    def __next__(self):
        if self._item < len(self._times) - 1:
            self._item += 1
            curr_time = self._times[self._item]
            curr_data = self.data[self.data['timestamp']==curr_time]
            self._update_state(curr_data)
            self._update_best_prices()
            return self._state
        else:
            raise(StopIteration)
    
    def _update_state(self, data: pd.DataFrame):
        self._state = self._state.append(data).sort_values('timestamp')
        self._state = self._state[~self._state.index.duplicated(keep='last')]
        self._state = self._state[self._state['size']!=0].sort_index(ascending=False)

    def _update_best_prices(self):
        best_ask_price = self._state[self._state['side']=='ask'].index.min()
        best_bid_price = self._state[self._state['side']=='bid'].index.max()
        self.best_ask = self._state.loc[best_ask_price]
        self.best_bid = self._state.loc[best_bid_price]


class TradesController:
    """
    Generator of Trades data from pd.DataFrame
    """
    def __init__(self, data: pd.DataFrame):
        self.data = data

    def __iter__(self):
        self._item = 0
        self._times = sorted(self.data['timestamp'].unique())
        init_time = self._times[self._item]
        self._state = self.data[self.data['timestamp']==init_time]
        return self

    def __next__(self):
        if self._item < len(self._times) - 1:
            self._item += 1
            curr_time = self._times[self._item]
            self._state = self.data[self.data['timestamp']==curr_time]
            return self._state
        else:
            raise(StopIteration)


class Enviroment:

    def __init__(self, obrderbook_data, trades_data):
        self.orderbook = iter(OrderBookController(obrderbook_data))
        self.trades = iter(TradesController(trades_data))
        self._get_start_position()
        self._item = 0

    def step(self):
        trade = next(self.trades)
        snapshot = self._find_snapshot(trade)
        self._item += 1
        return trade, snapshot
    
    def _find_snapshot(self, trade):
        

        return self.orderbook._state

    def _get_start_position(self):
        init_time_orderbook = self.orderbook._times[0]
        init_time_trades = self.trades._times[0]

        if init_time_orderbook < init_time_trades:
            curr_time_orderbook = init_time_orderbook
            while curr_time_orderbook < init_time_trades:
                next(self.orderbook)
                curr_time_orderbook = self.orderbook._state['timestamp'].iloc[0]

        if init_time_trades < init_time_orderbook:
            curr_time_trades = init_time_trades
            while curr_time_trades < init_time_orderbook:
                next(self.trades)
                curr_time_trades = self.trades._state['timestamp'].iloc[0]

        print('trades:\t', self.trades._state)
        print('orderbook:\t', self.orderbook._state)

DATA_PATH = os.path.join('.', 'data', 'sim_BTC-USDT')
files_list = sorted(glob(os.path.join(DATA_PATH, '*.csv')))

orderbook_data = pd.read_csv(files_list[0], sep=';', index_col=0)
trades_data = pd.read_csv(files_list[1], sep=';', index_col=0)

orderbook = OrderBookController(orderbook_data)
trades = TradesController(trades_data)
env = Enviroment(orderbook_data, trades_data)
print(env.step())
