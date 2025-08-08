from datetime import datetime
import pandas as pd
import numpy as np
import json
import os
from custom_queue import Queue
import multiprocessing

class EstablishDataframe:
    def __init__(self, data, window=9, rvol_window=20):
        self.data = data
        self.data = pd.read_json(self.data)
        self.data = pd.DataFrame(self.data)

        try:
            self.data["date"] = pd.to_datetime(self.data["date"])
            self.data.set_index(["date"], inplace=True)
        except KeyError:
            print("❌ 'date' column not found in data. Skipping datetime conversion and indexing.")

        self.queue = Queue(5)
        self.data = self.data_margin(self.data)
        self.window_size = pd.Timedelta(days=window)
        self.rvol_window = pd.Timedelta(days=rvol_window)

        for index in self.data.index:
            window_start = index - self.window_size
            window_data = self.data.loc[window_start:index, "close"]
            self.data.at[index, "mvAvg"] = window_data.mean()
            self.data["EMA"] = self.data["close"].ewm(span=window, adjust=False).mean()
            if self.queue.values:
                self.data.at[index, "RoC"] = self.calculate_roc(self.data.loc[index, "mvAvg"], self.queue.values[-1]["mvAvg"])
            else:
                self.data.at[index, "RoC"] = 0

            rvol_start = index - self.rvol_window
            rvol_data = self.data.loc[rvol_start:index, "volume"]
            avg_vol = rvol_data.mean()
            self.data.at[index, "rVol"] = self.calculate_rvol(self.data.loc[index, "volume"], avg_vol)
            if self.queue.values:
                self.data.at[index, "x_day_high"] = self.queue.max_value("high", 4)
            self.data.at[index, "pattern"] = None
            self.queue.enqueue(self.data.loc[index])

        
        
    def data_margin(self, dataframe):
        date_ranges = [
                ("2021-01-04", datetime.strftime(datetime.now(), "%Y-%m-%d")),
                ("2022-01-03", datetime.strftime(datetime.now(), "%Y-%m-%d")),
                ("2023-01-02", datetime.strftime(datetime.now(), "%Y-%m-%d")),
                ("2024-01-02", datetime.strftime(datetime.now(), "%Y-%m-%d")),
            ]
        for start_date, end_date in date_ranges:

            try:
                return dataframe.loc[start_date:end_date]
            except KeyError as e:
                print(f"KeyError: {e}. Trying next date range...")

        print("❌ No valid date range found. Returning original dataframe.")
        return dataframe
            
        
    def bull_or_bear(self, _open, _close):
        try:
            if _open < _close:
                return True
            if _close < _open:
                return False
            
        except TypeError as e:
            print(e)
            return None

    def calculate_roc(self, current_value, prev_value):
        
        return ((current_value - prev_value) / prev_value) * 100

    def calculate_rvol(self, current_vol, avg_vol):
        return current_vol/avg_vol

class Data():
    def __init__(self, cap):
        print(cap)
        if cap == "testing":
            self.data_path = r"testing_data"

        if cap == "all":
            self.data_path = r"all_cap_data"

        if cap == "small":
            self.data_path = r"small_cap_data"
       
        if cap =="mid":
            self.data_path = r"mid_cap_data"

        if cap == "data":
            self.data_path = r"data"

        self.queue = Queue(5)
        self.dates_queue = Queue(5)
        # self.file_queue = Queue(5)
        # self.dates_queue = Queue(5)

        # for n in sorted(os.listdir("stats")):
        #     self.file_queue.enqueue(n)

        # for n in sorted(os.listdir("dates")):
        #     self.file_queue.enqueue(n)

        self.commands = {
            "check_roc": self.check_roc,

            "new_low": self.new_low,

            "get_avg": self.get_avg
        }
        self.dataframes = self.multiprocess_dataframes(self.data_path)

    def find_new_highs(self):
        tickers = []
        for name, df in self.dataframes.items():
            if len(df) < 6:
                continue
            selected_rows = df["high"].iloc[-5:-1]
            last_row = df["high"].iloc[-1]
            if last_row >= max(selected_rows):
                current_close = df.iloc[-1]["close"]
                current_high = df.iloc[-1]["high"]
                prev_close = df.iloc[-2]["close"]
                prev_high = df.iloc[-2]["close"]
                if current_high > prev_high and current_close < prev_close:
                    tickers.append({name: df.iloc[1].to_dict()})
            
        return tickers


    def find_new_lows(self):
        tickers = []
        for name, df in self.dataframes.items():
            if len(df) < 6:
                continue
            selected_rows = df["low"].iloc[-5:-1]
            last_row = df["low"].iloc[-1]
            if last_row <= min(selected_rows):
                current_close = df.iloc[-1]["close"]
                current_low = df.iloc[-1]["low"]
                prev_close = df.iloc[-2]["close"]
                prev_low = df.iloc[-2]["low"]
                if current_close > prev_close and current_low < prev_low:
                    tickers.append({name: df.iloc[1].to_dict()})

        return tickers


    def splitup(self, lst, n):
        k, m = divmod(len(lst), n)
        return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

    def multiprocess_dataframes(self, file):
        print("Creating Dataframes!!!")
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        processes = []
        my_lists = self.splitup(os.listdir(file), 8)
        
        for idx, tickers in enumerate(my_lists):
            tickers = multiprocessing.Process(
                target=self.create_all_dataframes, 
                args=[tickers, self.data_path, return_dict, idx])
            tickers.start()
            processes.append(tickers)

        for process in processes:
            process.join()
        print(f"Dataframes established")
        final_result = {}
        for dict in return_dict.values():
            final_result.update(dict)
        return final_result
    
    def create_all_dataframes(self, tickers, base_path, return_dict, idx):
        result = {}
        for ticker in tickers:
            full_path = os.path.join(base_path, ticker)
            df = EstablishDataframe(full_path)
            result[ticker[:-5]] = df.data
        return_dict[idx] = result

# Fibonacci Retracement
    def fibonacci(self, zero, one):
        fibonacci_levels = [.236, .382, .5, .618, .786]
        
        high = max(zero, one)
        low = min(zero, one)
        diff = high - low
        nums = [low + (num * diff) for num in fibonacci_levels]
        return nums
# Number Search

    def data_search(self, condition, param=None):
        data = []
        dates = []
        counter = 0
        for name, df in self.dataframes.items():
            for index, row in df.iterrows():
                counter += 1
                if param is None:
                    if condition(row) is True:
                        dates.append(f"{df} {row.name}") 
                        data.append() 
                                                 
                if param:
                    if condition(row, param) is True:
                        dates.append(f"{df} {row.name}")
                        
        print(counter)
        return dates, data

# Value Search Functions

    def check_roc(self, row, num):
        if isinstance(num, str):
            num = int(num)
        if num > 0:
            if row["RoC"] > num:
                return True
        if num < 0:
            if row["RoC"] < num:
                return True
        return False

    def check_vol(self, row, num):
        if isinstance(num, str):
            num = int(num)
        if num > 0:
            if row["volume"] > num:
                return True
        if num < 0:
            if row["volume"] < num:
                return True
        return False
    
    def get_avg(self, data):
        if isinstance(data, list):
            try:
                return (sum(data) / len(data)) * 100
            except Exception as e:
                print(e)
        if isinstance(data, str):
            return self.read_stats()
                
# Pattern Search

    # Pattern search function tasks
    # iterate through dataframe and check queue for a pattern
    
    def pattern_search(self, pattern):
        instances = 0
        dates = {}
        for name, df in self.dataframes.items():
            one_ticker_dates = []
            for data in range(len(df)):
                
                self.queue.enqueue(df.iloc[data])
                if pattern() is True:
                    one_ticker_dates.append(self.queue.values[4].name)
                    instances += 1
                    # TEMP
                    print(name, self.queue.values[4].name, self.fibonacci(self.queue.values[4]["low"], self.queue.values[4]["close"]))
            dates[name] = one_ticker_dates
        date_file = self.dates_queue.dequeue()
        self.queue.enqueue(date_file)
        print(instances)
        self.write_dates(date_file, dates)
        return dates

# Pattern Functions
    
    def new_low(self):
        if len(self.queue.values) < 5:
            return False
        
        current_low = self.queue.values[4]["low"]
        new_low = self.queue.min_value("low")
        prev_high = self.queue.values[3]["high"]
        close = self.queue.values[4]["close"]

        if (
            current_low == new_low and
            close > prev_high
        ):
            return True
        
    def new_high(self):
        if len(self.queue.values) < 5:
            return False
        
        current_high = self.queue.values[4]["high"]
        new_high = self.queue.max_value("high")
        prev_close = self.queue.values[3]["close"]
        close = self.queue.values[4]["close"]

        if (
            current_high == new_high and
            close < prev_close
        ):
            return True

# data = Data("small")

# data.pattern_search(data.new_high)
# # data.pattern_search(data.new_low)
# dates, data = data.data_search(data.check_roc, -10)
# print(len(dates), len(data))

