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
        with open(r"tickers/AllCapTickers.json", "r") as file:
            data = json.load(file)
            self.all_cap_tickers = [item for item in data]
        

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
    
    async def get_symbol(self, symbol, session):
        url = f"https://eodhd.com/api/eod/{symbol}.US?api_token={self.token}&fmt=json"
        print(f"Getting data for ticker: {symbol}")
        
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

        return data

# Intraday Data
    async def get_one_hour(self, symbol, _from=None, to=None):
        if isinstance(_from, datetime):
            _from = int(_from.timestamp())
        if isinstance(to, datetime):
            to = int(to.timestamp())
        if to is None:
            to == _from + (86400 * 5)
        
        if _from:
            url = f"https://eodhd.com/api/intraday/{symbol}?interval=1h&api_token={self.token}&fmt=json&from={_from}&to={to}"

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
        
        with open(f"Trading/one_hour_data/{symbol}.json", "w") as file:
            json.dump(data, file)
        return data
    
    async def get_five_min(self, symbol, _from=None, to=None):
        if isinstance(_from, datetime):
            _from = int(_from.timestamp())
        if isinstance(to, datetime):
            to = int(to.timestamp())
        if to is None:
            to == _from + (86400 * 5)
        
        if _from:
            url = f"https://eodhd.com/api/intraday/{symbol}?interval=5m&api_token={self.token}&fmt=json&from={_from}&to={to}"

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
                
        with open(f"Trading/five_min_data/{symbol}.json", "w") as file:
            json.dump(data, file)
        return data
    
    async def get_one_min(self, symbol, _from, to=None):
        if isinstance(_from, datetime):
            _from = int(_from.timestamp())
        if isinstance(to, datetime):
            to = int(to.timestamp())
        if to is None:
            to == _from + (86400 * 5)
        
        if _from:
            url = f"https://eodhd.com/api/intraday/{symbol}?interval=1m&api_token={self.token}&fmt=json&from={_from}&to={to}"
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
        
        with open(f"Trading/one_min_data/{symbol}.json", "w") as file:
            json.dump(data, file)
        return data

    async def get_all_intraday(self, cap, tf="1h"):
        if tf == "1h":
            if cap == "small":
                for ticker in self.small_cap_tickers:
                    data = await self.get_one_hour(ticker)
            if cap == "mid":
                for ticker in self.mid_cap_tickers:
                    data = await self.get_one_hour(ticker)
            if cap == "large":
                for ticker in self.large_cap_tickers:
                    data = await self.get_one_hour(ticker)
            if cap == "all":
                for ticker in self.all_cap_tickers:
                    data = await self.get_one_hour(ticker)
        if tf == "5m":
            if cap == "small":
                for ticker in self.small_cap_tickers:
                    data = await self.get_five_min(ticker)
            if cap == "mid":
                for ticker in self.mid_cap_tickers:
                    data = await self.get_five_min(ticker)
            if cap == "large":
                for ticker in self.large_cap_tickers:
                    data = await self.get_five_min(ticker)
            if cap == "all":
                for ticker in self.all_cap_tickers:
                    data = await self.get_five_min(ticker)
        if tf == "1m":
            if cap == "small":
                for ticker in self.small_cap_tickers:
                    data = await self.get_one_min(ticker)
            if cap == "mid":
                for ticker in self.mid_cap_tickers:
                    data = await self.get_one_min(ticker)
            if cap == "large":
                for ticker in self.large_cap_tickers:
                    data = await self.get_one_min(ticker)
            if cap == "all":
                for ticker in self.all_cap_tickers:
                    data = await self.get_one_min(ticker)
                    
    async def get_watchlist_data(self, watchlist):
        print(watchlist)
        tasks = []
        if isinstance(watchlist, dict):
            async with aiohttp.ClientSession() as session:
                tasks = [
                    self.get_symbol(value["ticker"], session)
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
                tasks = [self.get_symbol(ticker, session) for ticker in watchlist]
                results = await asyncio.gather(*tasks)
                print(file)
                for ticker, data in zip(watchlist, results):
                    if data:
                        with open(f"{file}/{ticker}.json", "w") as f:
                            print(f)
                            json.dump(data, f)
                    
        else:
            raise TypeError("Incorrect data type passed into get_watchlist_data")

    async def get_symbol_data(self, ticker, file="Trading/data"):
        try:
            data = await self.get_symbol(ticker)
            os.makedirs(f"{os.path.dirname(os.path.abspath(__file__))}/{file}", exist_ok=True)
            if not data:
                    return
            with open(f"{file}/{ticker}.json", "w") as file:
                json.dump(data, file)

        except Exception as e:
            print(f"Error with ticker {ticker}! Error: {e}")
    
    async def get_watchlist(self, session, url):
        
        data = await self.fetch_json(session, url)

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
            return await self.get_symbol(symbol)
            
        async with aiohttp.ClientSession() as session:
            if isinstance(url, str):
                with open("Trading/stats/tickers.json", "r") as file:
                    data = file.read()
                    watchlist = [item.strip().strip('"') for item in data.strip(",").split(",")]

                data = await self.get_watchlist_data(session, watchlist)

    async def csv_file_reader(mkt_cap):
        tickers = []  
        with open(f'Trading/tickers/{mkt_cap}Tickers.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                print(row)
                tickers.append(row['Symbol'])


        with open(f'Trading/tickers/{mkt_cap}Tickers.json', 'w') as jsonfile:
            json.dump(tickers, jsonfile, indent=4)

# api = AsyncApiCaller()

# if __name__ == "__main__":
#     asyncio.run(api.get_watchlist_data(api.all_cap_tickers))


