# Where the dataframe instantiation objects are stored and maintained
import pandas as pd
import operator
from custom_queue import Queue
from datetime import datetime
import json

class EstablishDataframe:
    def __init__(self, data, nine_day_window=9, large_day_window=40, rvol_window=20):
        self.data = data
        self.data = pd.read_json(self.data)
        self.data = pd.DataFrame(self.data)

        try:
            self.data["date"] = pd.to_datetime(self.data["date"])
            self.data.set_index(["date"], inplace=True)
        except KeyError:
            self.data["timestamp"] = pd.to_datetime(self.data["timestamp"])
            self.data.set_index(["timestamp"], inplace=True)
        except KeyError:
            print("❌ 'date', 'timeframe', 'datetime' columns not found in data. Skipping datetime conversion and indexing.")
            
        self.queue = Queue(6)
        self.data = self.data_margin(self.data)
        self.window_size = pd.Timedelta(days=nine_day_window)
        self.large_window = pd.Timedelta(days=large_day_window)
        self.rvol_window = pd.Timedelta(days=rvol_window)


        self.data["mvAvg9"] = self.data["close"].rolling(window=nine_day_window).mean()
        self.data["mvAvg40"] = self.data["close"].rolling(window=large_day_window).mean()

        self.data["mvAvg9_prev"] = self.data["mvAvg9"].shift(1)
        

        self.data["avg_vol"] = self.data["volume"].rolling(window=self.rvol_window).mean()
        self.data["rVol"] = self.data["volume"] / self.data["avg_vol"]
        
        self.data["x_day_high"] = None
        self.data["pattern"] = None
        
        for index, row in self.data.iterrows():
            if self.queue.values:
                self.data.at[index, "x_day_high"] = self.queue.max_value("high", 4)
            self.queue.enqueue(row)
            
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
            


    def calculate_roc(self, current_value, prev_value):
        
        return ((current_value - prev_value) / prev_value) * 100

    def calculate_rvol(self, current_vol, avg_vol):
        return current_vol/avg_vol

class IntradayDataframe:
    def __init__(self, data, nine_day_window=9, large_day_window=40, rvol_window=20):
        self.data = data
        self.data = pd.read_json(self.data)
        self.data = pd.DataFrame(self.data)
        try:
            self.data["datetime"] = pd.to_datetime(self.data["datetime"])
            self.data.set_index(["datetime"], inplace=True)
        except KeyError:
            try:
                self.data["timestamp"] = pd.to_datetime(self.data["timestamp"])
                self.data.set_index(["timestamp"], inplace=True)
            except KeyError:
                print("❌ 'date' column not found in data. Skipping datetime conversion and indexing.")
                

        self.queue = Queue(6)
        self.data = self.data_margin(self.data)
        self.window_size = pd.Timedelta(days=nine_day_window)
        self.large_window = pd.Timedelta(days=large_day_window)
        self.rvol_window = pd.Timedelta(days=rvol_window)

        for index in self.data.index:
            self.data["mvAvg9"] = self.data["close"].rolling(window=nine_day_window).mean()
            self.data["mvAvg40"] = self.data["close"].rolling(window=large_day_window).mean()

            self.data["EMA"] = self.data["close"].ewm(span=nine_day_window, adjust=False).mean()
            self.data["EMA36"] = self.data["close"].ewm(span=large_day_window, adjust=False).mean()
            if self.queue.values:
                self.data.at[index, "RoC"] = self.calculate_roc(self.data.loc[index, "mvAvg9"], self.queue.values[-1]["mvAvg9"])
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
        self.final_days = self.data.iloc[-20:-1]

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
            
    def filter(self, params):
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
            if func(params[0], params[2]):
                return params[0]
        return None

    def calculate_roc(self, current_value, prev_value):
        
        return ((current_value - prev_value) / prev_value) * 100

    def calculate_rvol(self, current_vol, avg_vol):
        return current_vol/avg_vol