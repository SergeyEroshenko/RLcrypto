import os
import asyncio
import websockets
import ssl
import json
import zlib
from decimal import Decimal
from datetime import timedelta
import pandas as pd


class Connector:

    uri = 'wss://real.okex.com:8443/ws/v3'

    def __init__(self):
        self.connection = None

    async def run(self):
        # открываем соединение
        self.connection = await websockets.connect(
            uri=self.uri,
            ssl=ssl.SSLContext(ssl.PROTOCOL_TLS)
        )

    async def subscribe_orderbook(self, symbol):
        # подписываемся на orderbook
        msg = json.dumps({
                "op": "subscribe",
                "args": [f"spot/depth_l2_tbt:{symbol}"]
                })
        await self.connection.send(msg)

    async def subscribe_trades(self, symbol):
        # подписываемся на trades
        msg = json.dumps({
                "op": "subscribe",
                "args": [f"spot/trade:{symbol}"]
                })
        await self.connection.send(msg)

    async def handler(self, parser):
        # получатель собщений из установленного соединения
        async for message in self.connection:
            parser.parse(message)


class Parser:

    def __init__(self, stop_price, stop_time, save_path):
        self.stop_price = stop_price
        self.stop_time = stop_time
        self.save_path = save_path
        self.trades = None
        self.orderbook = None
        if not os.path.exists(save_path):
            os.makedirs(save_path)

    def parse(self, message):
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)  # see above
        inflated = decompress.decompress(message)
        inflated += decompress.flush()
        message = json.loads(inflated)
        if 'table' not in message.keys():
            return None
        data_type = message['table'].split('/')[-1]

        if data_type == 'trade':
            self.parse_trades(message['data'])
            self.check_stop()
        elif data_type == 'depth_l2_tbt':
            self.parse_orderbook(message['data'])

    def parse_trades(self, data):
        data = self.dtypes_correct(data)

        if self.trades is None:
            self.trades = data
        else:
            self.trades = self.trades.append(data, ignore_index=True)

    def parse_orderbook(self, data):
        data = data[0]
        columns = ['price', 'size', 'del_orders', 'orders']
        asks = pd.DataFrame(data['asks'], columns=columns)\
            .loc[:, ['price', 'size']]
        bids = pd.DataFrame(data['bids'], columns=columns)\
            .loc[:, ['price', 'size']]
        asks['side'] = 'ask'
        bids['side'] = 'bid'
        df = asks.append(bids)
        df['timestamp'] = data['timestamp']
        df = self.dtypes_correct(df)

        if self.orderbook is None:
            self.orderbook = df
        else:
            self.orderbook = self.orderbook.append(df, ignore_index=True)

    def check_stop(self):
        start_price = Decimal(self.trades.iloc[0]['price'])\
            .quantize(Decimal('0.1'))
        start_time = self.trades.iloc[0]['timestamp']
        curr_price = Decimal(self.trades.iloc[-1]['price'])\
            .quantize(Decimal('0.1'))
        curr_time = self.trades.iloc[-1]['timestamp']
        delta_price = abs(curr_price - start_price)
        delta_time = curr_time - start_time
        if (delta_price >= self.stop_price) | (delta_time >= self.stop_time):
            self.trades.to_csv(
                os.path.join(self.save_path, f'{start_time}_trades.csv'),
                sep=';'
            )
            self.orderbook.to_csv(
                os.path.join(self.save_path, f'{start_time}_orderbook.csv'),
                sep=';'
            )
            exit()

    @staticmethod
    def dtypes_correct(data):
        data_types = {
            'timestamp': 'datetime64[ns]',
            'price': 'object',
            'size': 'object'
        }
        data = pd.DataFrame(data).astype(data_types)
        return data


async def main(conn, symbol):

    await conn.run()
    tasks = [
        conn.subscribe_trades(symbol),
        conn.subscribe_orderbook(symbol),
        conn.handler(parser)
    ]
    await asyncio.gather(*tasks)


if __name__ == '__main__':

    save_path = './data/sim_BTC-USDT'
    symbol = 'BTC-USDT'
    stop_price = 100
    stop_time = timedelta(minutes=1)

    conn = Connector()
    parser = Parser(stop_price, stop_time, save_path)

    asyncio.run(main(conn, symbol))
