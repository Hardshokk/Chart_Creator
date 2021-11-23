import os
import asyncio
import threading
import datetime as dt
import multiprocessing as mp

import pandas as pd
import aiohttp
import mplfinance as mpf

pd.set_option("max_rows", 1000)


crypto_list = [
    'C98', 'LUNA', 'CTK', 'OMG', 'SC', 'HBAR', 'SXP', 'DENT', 'TOMO', 'SOL', 'BEL', 'ENJ',
    'RAY', 'ONT', 'ZIL', 'XTZ', 'MANA', 'NEO', 'OCEAN', 'SFP', 'COTI', 'MTL', 'FLM',
    'KNC', 'BAT', 'XEM', 'ADA', 'ZRX', 'NEAR', 'CHZ', 'KAVA', 'CVC',
    'THETA', 'RLC', 'STMX', 'LINA', 'ZEN', 'BLZ',
    'TRX', 'SRM', 'ETH', 'BNB', 'ALPHA', 'ICX', 'LINK',
    'BAND', 'AAVE', 'DGB', 'BCH', 'BTT', 'BTC', 'WAVES', 'AVAX', 'LTC', 'ALGO', 'UNI',
    'LIT',  'XMR', 'UNFI', 'ONE', 'DOGE', 'KSM', 'MKR', 'RVN', 'BAL', 'COMP',
    'GTC', 'SUSHI', 'AKRO', 'DASH', 'SNX', 'BAKE', 'TRB', 'DODO', 'FTM', 'RUNE'
]
symbol_list = [x + 'USDT' for x in crypto_list]


class DataGetter(threading.Thread):
    def __init__(self, data_queue):
        threading.Thread.__init__(self)
        self.qv_bars = 500
        self.interval_list = ["5m", "15m", "1h"]
        self.queue = data_queue

    def get_start_time(self, interval):
        """Основной вариант получения необходимого количества баров таймфрейма."""
        if interval == "15m":
            delta = self.qv_bars * 15
        elif interval == "5m":
            delta = self.qv_bars * 5
        elif interval == "1h":
            delta = self.qv_bars * 60
        else:
            raise Exception("Полученный интервал не определен.")
        dn = dt.datetime.now() - dt.timedelta(minutes=delta)
        result = str(int(dn.timestamp() * 1000))
        return result

    async def requester(self, session, symbol, interval):
        try:
            start_time = self.get_start_time(interval)
            url = f"""https://api3.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&startTime={start_time}"""
            response = await session.get(url)
            text = await response.text()
            self.queue.put({'klines': text, 'symbol': symbol, 'interval': interval})
        except Exception as e:
            print(f"Пропускаю {symbol}, причина {e}")

    async def main_controller(self):
        tasks = []
        async with aiohttp.ClientSession() as session:
            for symbol in symbol_list:
                for interval in self.interval_list:
                    tasks.append(self.requester(session, symbol, interval))
            await asyncio.gather(*tasks)

    def run(self):
        asyncio.run(self.main_controller())
        self.queue.put('end\r\n')
        print("---", self.queue.qsize())


class PrinterCharts(mp.Process):
    def __init__(self, data_queue, number):
        super().__init__()
        self.queue = data_queue
        self.name_process = f"process__{number}"

    @staticmethod
    def main(data):
        dn = dt.datetime.now()
        candle_columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                          'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
                          'taker_buy_quote_asset_volume', 'ignore']
        df = pd.DataFrame(data=eval(data['klines']), columns=candle_columns)
        df['open'] = pd.to_numeric(df['open'], downcast='float')
        df['high'] = pd.to_numeric(df['high'], downcast='float')
        df['low'] = pd.to_numeric(df['low'], downcast='float')
        df['close'] = pd.to_numeric(df['close'], downcast='float')
        df['volume'] = pd.to_numeric(df['volume'], downcast='float')
        df['date'] = df.apply(lambda row: pd.Timestamp(row['open_time'], unit='ms'), axis=1)
        df_exp = df.copy()
        df_exp.index = df_exp['date']
        df_for_chart = df_exp.loc[:, ['open', 'high', 'low', 'close', 'volume']]
        dir_name = f"{dn.strftime(format='%Y-%m-%d')}_{data['interval']}"
        if not os.path.isdir(dir_name):
            os.mkdir(dir_name)
        mpf.plot(df_for_chart, type='candle', volume=True, mav=(7, 25, 100, 200, 300, 400),
                 savefig=dict(
                     fname=f"{dir_name}\\{data['symbol']}_{data['interval']}_{dn.strftime(format='%Y-%m-%d_%H-%M')}.jpg",
                     dpi=300, pad_inches=0.7))

    def run(self):
        while True:
            try:
                item = self.queue.get(timeout=10)
            except Exception as e:
                print(f"Завершаю поток т.к. очередь пуста в течение 10 сек. ({e})")
                break
            if item == 'end\r\n':
                print("Завершаю очередь и поток.")
                break
            print(f"{self.name_process} -------")
            self.main(item)


if __name__ == '__main__':
    main_queue = mp.Queue()

    process_1 = PrinterCharts(main_queue, 1)
    process_1.start()
    process_2 = PrinterCharts(main_queue, 2)
    process_2.start()

    data_getter = DataGetter(main_queue)
    data_getter.start()

