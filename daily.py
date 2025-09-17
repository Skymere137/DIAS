from datetime import datetime
import schedule
from candlesticks import Data
import json
from data import AsyncApiCaller
import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager

api = AsyncApiCaller()

async def main():
    
    # await acquire_all_small_cap()
    get_dataframes()

async def acquire_all_small_cap():
    api = AsyncApiCaller()
    for ticker in api.small_cap_tickers:
        for func in api.data_acquiration_arr:
            await api.get_one_week(func, "s", ticker)

async def acquire_all_mid_cap():
    api = AsyncApiCaller()
    for ticker in api.mid_cap_tickers:
        for func in api.data_acquiration_arr:
            await api.get_one_week(func, "m", ticker)

# async def acquire_all_large_cap():
#     api = AsyncApiCaller()
#     for ticker in api.large_cap_tickers:
#         for func in api.data_acquiration_arr:
#             await api.get_one_week(func, "l", ticker)

# asyncio.run(get_my_data())
def get_dataframes():
    global small_cap_data
    small_cap_data = Data("small")
    global mid_cap_data
    mid_cap_data = Data("mid")
    global one_hr_small
    one_hr_small = Data("small", "1hr")
    global five_min_small
    five_min_small = Data("small", "5min")
    global one_min_small
    one_min_small = Data("small", "1min")

def async_scheduler(task):
    asyncio.create_task(task)

def schedulers():
    try:
        schedule.every().day.at("04:00").do(lambda: async_scheduler(acquire_all_small_cap()))
    except Exception as e:
        print(e)
    try:
        schedule.every().day.at("05:00").do(lambda: get_dataframes())
    except Exception as e:
        print(e)
    
async def run_scheduler():
    schedulers()
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting app and scheduler...")
    await main()

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

@app.get("/1hrUptrendS")
async def uptrending():
    uptrends = one_hr_small.find_green_crossovers()
    return one_hr_small.json_safe(uptrends)

@app.get("/5minUptrendS")
async def uptrending():
    uptrends = five_min_small.find_green_crossovers()
    return five_min_small.json_safe(uptrends)

@app.get("/1minUptrendS")
async def uptrending():
    uptrends = one_min_small.find_green_crossovers()
    return one_min_small.json_safe(uptrends)

@app.get("/s_new_highs")
async def new_highs_json():
    new_highs = small_cap_data.find_new_highs()
    return small_cap_data.json_safe(new_highs)

@app.get("/s_new_lows")
async def new_lows_json():
    new_lows = small_cap_data.find_new_lows()
    return small_cap_data.json_safe(new_lows)

@app.get("/s_green_crossover")
async def green_crossover_json():
    crossovers = small_cap_data.find_green_crossovers()
    return small_cap_data.json_safe(crossovers)

@app.get("/m_new_highs")
async def new_highs_json():
    new_highs = mid_cap_data.find_new_highs()
    return mid_cap_data.json_safe(new_highs)

@app.get("/m_new_lows")
async def new_lows_json():
    new_lows = mid_cap_data.find_new_lows()
    return mid_cap_data.json_safe(new_lows)