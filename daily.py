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

def get_dataframes():
    global small_cap_data
    small_cap_data = Data("small")
def print_statement():
    print("Hello World!!!")

small_cap_data = Data("small")

schedule.every().day.at("05:00").do(lambda: api.get_watchlist_data)
schedule.every().day.at("05:30").do(lambda: print_statement)

async def run_scheduler():
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting app and scheduler...")
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

@app.get("/new_lows")
async def new_lows_json():
    new_lows = small_cap_data.find_new_lows()
    return new_lows







# def run_schedule():
#     ops = Operations()
#     api = AsyncApiCaller()
   
    
    # try:
    #     schedule.every().day.at("01:06").do(lambda: asyncio.run(api.get_watchlist_data(api.small_cap_tickers)))
    # except Exception as e:
    #     print("Process could_not be run!!!")
    #     print(e)
    # try:
    #     schedule.every().day.at("00:34").do(lambda: ops.get_small_cap())
    # except Exception as e:
    #     print("Process 2 could_not be run!!!")
    #     print(e)
    
    # try:
    #     schedule.every().day.at("00:34").do(lambda: ops.run())     
    # except Exception as e:
    #     print("Process 3 could_not be run!!!")
    #     print(e)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
        

# run_schedule()