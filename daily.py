from datetime import datetime
import schedule
from flask import Flask, jsonify
from candlesticks import Data
import json
from data import AsyncApiCaller
import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager

api = AsyncApiCaller()

# async def main():
    
#     print("Hello World!")
#     await api.get_watchlist_data(api.small_cap_tickers)
#     await api.get_watchlist_data(api.mid_cap_tickers)
#     get_dataframes()

#     print("Goodbye World!")

def get_dataframes():
    global small_cap_data
    small_cap_data = Data("small")
    global mid_cap_data
    mid_cap_data = Data("mid")

def async_scheduler(task):
    asyncio.create_task(task)

def schedulers():
    try:
        schedule.every().day.at("05:00").do(lambda: async_scheduler(api.get_watchlist_data(api.small_cap_tickers)))
    except Exception as e:
        print(e)
    try:
        schedule.every().day.at("05:15").do(lambda: async_scheduler(api.get_watchlist_data(api.mid_cap_tickers)))
    except Exception as e:
        print(e)
    try:
        schedule.every().day.at("05:30").do(lambda: get_dataframes())
    except Exception as e:
        print(e)
    try:
        schedule.every().day.at("06:00").do(lambda: get_dataframes())
    except Exception as e:
        print(e)
async def run_scheduler():

    while True:
        schedulers()
        schedule.run_pending()
        await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting app and scheduler...")
    # await main()

    task = asyncio.create_task(run_scheduler())
    yield
    print("Shutting down...")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("Scheduler stopped.")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "Hello, FastAPI!"}

@app.get("/newHigh_smallCap")
async def new_highs_json():
    new_highs = small_cap_data.find_new_highs()
    return new_highs

@app.get("/new_lows")
async def new_lows_json():
    new_lows = small_cap_data.find_new_lows()
    return new_lows

