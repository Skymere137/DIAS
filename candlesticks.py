import math
from datetime import datetime
import pandas as pd
import numpy as np
import json
import operator
import os
from custom_queue import Queue
import multiprocessing
from DataFrames import EstablishDataframe


class Data():
    def __init__(self, cap, tf="daily"):
        print(cap)
        if tf != "daily":
            self.intraday = True
        else: 
            self.intraday = False

        capitalizations = {
            "large": "l",
            "mid": "m",
            "small": "s"
        }
        tfs = {
            "daily": "daily",
            "1hr": "one_hour",
            "5min": "five_min",
            "1min": "one_min"
        }
        try:
            for key, value in capitalizations.items():
                if key == cap:
                    cap_size = value

            for key, value in tfs.items():
                if tf == key:
                    tf = value
        except:
            return print("Market Cap or Tf value not found in dict!")
        self.data_path = f"{tf}_{cap_size}"
        
        print(self.data_path)
        self.queue = Queue(6)

        self.commands = {
            "check_roc": self.check_roc,

            "new_low": self.new_low,

            "get_avg": self.get_avg
        }
        self.dataframes = self.multiprocess_dataframes(self.data_path)
        
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

    def find_green_crossovers(self):
        tickers = []
        for name, df in self.dataframes.items():
            if len(df) < 36:
                continue
            small_averages = df.iloc[-12:-1]
            large_averages = df.iloc[-12:-1]
            
            if small_averages["mvAvg9"][-5] < large_averages["mvAvg40"][-5]:
                if small_averages["mvAvg9"][-4] > small_averages["mvAvg9"][-5] and large_averages["mvAvg40"][-4] < large_averages["mvAvg40"][-5]:
                    if small_averages["mvAvg9"][-3] > small_averages["mvAvg9"][-4] and large_averages["mvAvg40"][-3] < large_averages["mvAvg40"][-4]:
                        if small_averages["mvAvg9"][-2] > small_averages["mvAvg9"][-3] and large_averages["mvAvg40"][-2] < large_averages["mvAvg40"][-3]:
                            if small_averages["mvAvg9"][-1] > small_averages["mvAvg9"][-2] and large_averages["mvAvg40"][-1] < large_averages["mvAvg40"][-2]:
                                tickers.append({name: df.iloc[-1].to_dict()})
                                print(name)
                                print(small_averages["mvAvg9"][-5], large_averages["mvAvg40"][-5])
        return tickers
    
    def find_roc_opportunities(self):
        print("Searching")
        tickers = []
        for name, df in self.dataframes.items():
            if len(df) < 6:
                continue
            if self.filter(df.iloc[-1], ["RoC", ">", 1]):
                tickers.append(name)

            print(name, df["RoC"])
        return tickers
            
    def find_new_highs(self):
        tickers = []
        for name, df in self.dataframes.items():
            if len(df) < 6:
                continue
            print(df)
            selected_rows = df["high"].iloc[-6:-1]
            last_row = df["high"].iloc[-1]
            prev_highs = max(df.iloc[-6:-1]["high"])
            if last_row >= max(selected_rows):
                current_close = df.iloc[-1]["close"]
                current_high = df.iloc[-1]["high"]
                prev_low = df.iloc[-2]["low"]
                if current_high >= prev_highs:
                    if current_close < prev_low:
                        tickers.append({name: df.iloc[-1].to_dict()})
            
        return tickers


    def find_new_lows(self):
        tickers = []
        for name, df in self.dataframes.items():
            if len(df) < 6:
                continue
            selected_rows = df["low"].iloc[-6:-1]
            last_row = df["low"].iloc[-1]
            prev_lows = min(df.iloc[-6:-1]["lows"])
            if last_row <= min(selected_rows):
                current_close = df.iloc[-1]["close"]
                current_low = df.iloc[-1]["low"]
                prev_high = df.iloc[-2]["high"]
                if current_low >= prev_lows:
                    if current_close > prev_high:
                        tickers.append({name: df.iloc[-1].to_dict()})

        return tickers

    def splitup(self, lst, n):
        k, m = divmod(len(lst), n)
        return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

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
        
        return dates

# Pattern Functions
    def json_safe(self, obj):
        if isinstance(obj, dict):
            return {k: self.json_safe(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.json_safe(v) for v in obj]
        elif isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
        return obj

    def uptrend_below_avg(self):
        if self.queue.values["mvAvg9"].iloc[0] >= self.queue.values["mvAvg40"].iloc[0]:
            return False
    
        for i in range(1, 6):
            print(self.queue.values["mvAvg9"].iloc[i], self.queue.values["mvAvg9"].iloc[i-1])
            if self.queue.values["mvAvg9"].iloc[i] <= self.queue.values["mvAvg9"].iloc[i-1]:
                return False 
        
    
    def new_low(self):
        if len(self.queue.values) < 6:
            return False
        
        current_low = self.queue.values[5]["low"]
        new_low = self.queue.min_value("low")
        prev_high = self.queue.values[4]["high"]
        close = self.queue.values[5]["close"]

        if (
            current_low <= new_low and
            close > prev_high
        ):
            return True
        
    def new_high(self):
        if len(self.queue.values) < 6:
            return False
        
        current_high = self.queue.values[5]["high"]
        new_high = self.queue.max_value("high")
        prev_low = self.queue.values[4]["low"]
        close = self.queue.values[5]["close"]

        if (
            current_high >= new_high and
            close < prev_low
        ):
            return True

    def filter(self, row, params):
        self.dataframes
        if not isinstance(params, list):
            raise TypeError("Non list type found in parameter")  
        ops = {
            "==": operator.eq,
            "!=": operator.ne,
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le
        }
        if params[1] not in ops:
            raise ValueError("Incorrect operand type in method parameters")
        else:
            func = ops[params[1]]
            params[0] = row[params[0]]
            if func(params[0], params[2]):
                return params[0]
        return None
