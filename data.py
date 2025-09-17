import requests
import asyncio
import aiohttp
import pandas as pd
import csv
import json
import os
from datetime import datetime


class AsyncApiCaller():
    def __init__(self):
        self.token = os.environ["dataToken"]

        self.micro_cap_url = f"""https://eodhd.com/api/screener?api_token={self.token}&sort=market_capitalization.desc&filters=[
            ["market_capitalization", "<", 300000000],
            ["exchange", "=", "NYSE"],
            ["adjusted_close", ">", "1"]
            ]&limit=100&offset=0"""
        
        self.small_cap_url = f"""https://eodhd.com/api/screener?api_token={self.token}&sort=market_capitalization.desc&filters=[
            ["market_capitalization", "<", 2000000000],
            ["market_capitalization", ">", 300000000],
            ["exchange", "=", "NYSE"],
            ["adjusted_close", ">", "1"]
            ]&limit=100&offset=0"""
        
        self.mid_cap_url = f"""https://eodhd.com/api/screener?api_token={self.token}&sort=market_capitalization.desc&filters=[
            ["market_capitalization", ">", 2000000000],
            ["market_capitalization", "<", 10000000000],
            ["exchange", "=", "NYSE"],
            ["adjusted_close", ">", "1"]
            ]&limit=100&offset=0"""
        
        self.large_cap_url = f"""https://eodhd.com/api/screener?api_token={self.token}&sort=market_capitalization.desc&filters=[
            ["market_capitalization", ">", 10000000000],
            ["exchange", "=", "NYSE"],
            ["adjusted_close", ">", "1"]
            ]&limit=100&offset=0"""
        
        with open(r"tickers/smallTickers.json", "r") as file:
            data = json.load(file)
            self.small_cap_tickers = [item for item in data]
        with open(r"tickers/midTickers.json", "r") as file:
            data = json.load(file)
            self.mid_cap_tickers = [item for item in data] 
        # with open(r"tickers/largeTickers.json", "r") as file:
        #     data = json.load(file)
        #     self.large_cap_tickers = [item for item in data] 
        with open(r"tickers/AllCapTickers.json", "r") as file:
            data = json.load(file)
            self.all_cap_tickers = [item for item in data]
        self.data_acquiration_arr = [
            self.get_one_hour,
            self.get_five_min,
            self.get_one_min
        ]
        
    async def fetch_json(self, session, url, retries=3):
        tickers = []
        for attempt in range(retries):
            try:
                async with session.get(url) as response:
                    # Handle rate limiting
                    if response.status == 429:
                        
                        print(f"Rate limit hit (429), retrying in {60}s...")
                        await asyncio.sleep(60)
                        continue
                    tickers.append(url)
                    print(f"Successfully fetched data from: {url}")
                    # Check for proper content type
                    if 'application/json' not in response.headers.get('Content-Type', ''):
                        print(f"Unexpected content type: {response.headers.get('Content-Type')}")
                        text = await response.text()
                        print(f"Response content (truncated): {text[:200]}")
                        return None
                    return await response.json()

            except Exception as e:
                print(f"Exception during fetch: {e}")
                await asyncio.sleep(2)  # wait before retrying
        print(f"Failed after {retries} retries: {url}")
        return None

    async def get_daily_data(self, symbol, trgt_dir, session):
        url = f"https://eodhd.com/api/eod/{symbol}.US?api_token={self.token}&fmt=json"
        async with aiohttp.ClientSession() as session:
            try:
                data = await self.fetch_json(session, url)
            except aiohttp.ClientResponseError as e:
                print(f"HTTP error: {e}")
                data = None
            except aiohttp.ClientError as e:
                print(f"Request error: {e}")
                data = None
            except Exception as e:
                print(f"Unexpected error: {e}")
                data = None
        
        with open(f"{trgt_dir}/{symbol}.json", "w") as file:
            json.dump(data, file)
        return data
    
# Intraday Data
    async def get_one_hour(self, symbol, trgt_dir, _from=None, to=None):
        if isinstance(_from, datetime):
            _from = int(_from.timestamp())
        if isinstance(to, datetime):
            to = int(to.timestamp())
        if to is None:
            to == _from + (86400 * 5)
        
        if _from:
            url = f"https://eodhd.com/api/intraday/{symbol}.US?interval=1h&api_token={self.token}&fmt=json&from={_from}&to={to}"

        async with aiohttp.ClientSession() as session:
            try:
                data = await self.fetch_json(session, url)
            except aiohttp.ClientResponseError as e:
                print(f"HTTP error: {e}")
                data = None
            except aiohttp.ClientError as e:
                print(f"Request error: {e}")
                data = None
            except Exception as e:
                print(f"Unexpected error: {e}")
                data = None
        
        with open(f"one_hour_{trgt_dir}/{symbol}.json", "w") as file:
            json.dump(data, file)
        return data
    
    async def get_five_min(self, symbol, trgt_dir, _from=None, to=None):
        if isinstance(_from, str):
            _from = datetime.strptime(_from, "%Y-%m-%d")
        if isinstance(to, str):
            to = datetime.strptime(to, "%Y-%m-%d")
            
        if isinstance(_from, datetime):
            _from = int(_from.timestamp())
        if isinstance(to, datetime):
            to = int(to.timestamp())
        if to is None:
            to == _from + (86400 * 5)
        
        if _from:
            url = f"https://eodhd.com/api/intraday/{symbol}.US?interval=5m&api_token={self.token}&fmt=json&from={_from}&to={to}"

        async with aiohttp.ClientSession() as session:
            try:
                data = await self.fetch_json(session, url)
            except aiohttp.ClientResponseError as e:
                print(f"HTTP error: {e}")
                data = None
            except aiohttp.ClientError as e:
                print(f"Request error: {e}")
                data = None
            except Exception as e:
                print(f"Unexpected error: {e}")
                data = None
                
        with open(f"five_min_{trgt_dir}/{symbol}.json", "w") as file:
            json.dump(data, file)
        return data
    
    async def get_one_min(self, symbol, trgt_dir, _from, to=None):
        if isinstance(_from, datetime):
            _from = int(_from.timestamp())
        if isinstance(to, datetime):
            to = int(to.timestamp())
        if to is None:
            to == _from + (86400 * 5)
        if _from:
            url = f"https://eodhd.com/api/intraday/{symbol}.US?interval=1m&api_token={self.token}&fmt=json&from={_from}&to={to}"
        async with aiohttp.ClientSession() as session:
            try:
                data = await self.fetch_json(session, url)
            except aiohttp.ClientResponseError as e:
                print(f"HTTP error: {e}")
                data = None
            except aiohttp.ClientError as e:
                print(f"Request error: {e}")
                data = None
            except Exception as e:
                print(f"Unexpected error: {e}")
                data = None
        
        with open(f"one_min_{trgt_dir}/{symbol}.json", "w") as file:
            json.dump(data, file)
        return data
    
    async def get_one_day(self, tf, trgt_dir, symbol):
        today = int(datetime.now().timestamp())
        yesterday = int(datetime.now().timestamp()) - 86400
        return tf(symbol, trgt_dir, yesterday, today)

    async def get_one_week(self, tf, trgt_dir, symbol):
        today = int(datetime.now().timestamp())
        one_week_ago = int(datetime.now().timestamp()) - (86400 * 7)
        return await tf(symbol, trgt_dir, one_week_ago, today)
    
    async def get_one_month(self, tf, symbol):
        today = int(datetime.now().timestamp())
        one_month_ago = int(datetime.now().timestamp()) - (86400 * 30)
        return await tf(symbol, one_month_ago, today)
    
    async def get_one_year(self, tf, trgt_dir, symbol):
        today = int(datetime.now().timestamp())
        one_year_ago = int(datetime.now().timestamp()) - (86400 * 365)
        return await tf(symbol, trgt_dir, one_year_ago, today)
    
    async def more_than_one_year(self, tf, trgt_dir, symbol, year_num=1):
        today = int(datetime.now().timestamp())
        num_of_years = year_num * 365
        years_ago = int(datetime.now().timestamp()) - (86400 * year_num)
        return await tf(symbol, trgt_dir, years_ago, today)
    
    async def get_watchlist_data(self, watchlist):
        print(watchlist)
        tasks = []
        if isinstance(watchlist, dict):
            async with aiohttp.ClientSession() as session:
                tasks = [
                    self.get_daily_data(value["ticker"], session)
                    for key, value in watchlist.items()
                ]
                results = await asyncio.gather(*tasks)
            return results
        if isinstance(watchlist, list):
            if watchlist == self.small_cap_tickers:
                file = r"small_cap_data"
            if watchlist == self.mid_cap_tickers:
                file = r"mid_cap_data"
            # if watchlist == self.large_cap_tickers:
            #     file = r"Trading/large_cap_data"
            if watchlist == self.all_cap_tickers:
                file = r"all_cap_data"
            else: 
                print(watchlist)

            async with aiohttp.ClientSession() as session:
                tasks = [self.get_daily_data(ticker, session) for ticker in watchlist]
                results = await asyncio.gather(*tasks)
                print(file)
                for ticker, data in zip(watchlist, results):
                    if data:
                        with open(f"{file}/{ticker}.json", "w") as f:
                            print(f)
                            json.dump(data, f)
                    
        else:
            raise TypeError("Incorrect data type passed into get_watchlist_data")

    async def get_daily_data_data(self, ticker, file="data"):
        try:
            data = await self.get_daily_data(ticker)
            os.makedirs(f"{os.path.dirname(os.path.abspath(__file__))}/{file}", exist_ok=True)
            if not data:
                    return
            with open(f"{file}/{ticker}.json", "w") as file:
                json.dump(data, file)

        except Exception as e:
            print(f"Error with ticker {ticker}! Error: {e}")
    
    async def get_watchlist(self, url):
        async with aiohttp.ClientSession() as session:
            data = await self.fetch_json(session, url)
            print(data)
            watchlist = {
                item["name"]: {
                    "ticker": item["code"],
                    "mkt_cap": item["market_capitalization"],
                    "current_price": item["adjusted_close"],
                    "volume": item["avgvol_200d"],
                    "date": item["last_day_data_date"],
                }
                for item in data["data"]
            }
        return watchlist


    

    async def data_collection(self, symbol=None, url=None):
        if isinstance(symbol, str):
            return await self.get_daily_data(symbol)
            
        async with aiohttp.ClientSession() as session:
            if isinstance(url, str):
                with open("Trading/stats/tickers.json", "r") as file:
                    data = file.read()
                    watchlist = [item.strip().strip('"') for item in data.strip(",").split(",")]

                data = await self.get_watchlist_data(session, watchlist)

    async def csv_file_reader(self, mkt_cap):
        tickers = []  
        with open(f'tickers/{mkt_cap}Tickers.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                print(row)
                tickers.append(row['Symbol'])


        with open(f'tickers/{mkt_cap}Tickers.json', 'w') as jsonfile:
            json.dump(tickers, jsonfile, indent=4)
