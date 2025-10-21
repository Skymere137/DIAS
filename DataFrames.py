# Where the dataframe instantiation objects are stored and maintained
import pandas as pd
import operator
from custom_queue import Queue
from datetime import datetime
import json

class EstablishDataframe:
    def __init__(self, data):
        self.data = data
    
        self.data = pd.read_json(self.data)
        self.data = pd.DataFrame(self.data)
        if self.data.empty:
            print("❌ DataFrame is empty.")
            return
        
        try:
            self.data["datetime"] = pd.to_datetime(self.data["datetime"])
            self.data.set_index(["datetime"], inplace=True)
        except KeyError:
            try:
                self.data["timestamp"] = pd.to_datetime(self.data["timestamp"])
                self.data.set_index(["timestamp"], inplace=True)
            except KeyError:
                
                print("❌ 'date' column not found in data. Skipping datetime conversion and indexing.")
                return

        self.data = self.data_margin(self.data)

        required_cols = ["open", "high", "low", "close", "volume"]
        
        self.data = self.data.dropna(subset=required_cols)


        try:
            self.data["sma9"] = self.data["close"].rolling(window=9).mean()
            self.data["sma40"] = self.data["close"].rolling(window=40).mean()
        except Exception as e:
            print(e)

        try:
            self.data["avgVol"] = self.data["volume"].rolling(window=50).mean()
        except Exception as e:
            print(e)
        try:
            self.data["priceVar"] = [(self.data.loc[ei, "close"] - self.data.loc[ei, "sma40"]) ** 2 for ei in self.data.index]
        except Exception as e:
            print(e)
        try:
            self.data["chg%"] = (((self.data["close"] - self.data["open"]).abs()) / self.data["open"]) * 100
        except Exception as e:
            print(e)

    
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
            
