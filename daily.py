import requests
from datetime import datetime
import schedule
from candlesticks import Data
import json
from data import AsyncApiCaller
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel
from contextlib import asynccontextmanager

# Data acquiration
api = AsyncApiCaller()

with open("strats.json", "r") as file:
    strategies = json.load(file)
    

# VERY IMPORTANT!!!
global_model = {
    "strat": "Default",
    "cap": "small",
    "tf": "one_hr"
}

class Model(BaseModel):
    strat: str
    cap: str
    tf: str
    
async def main():
    
    # await acquire_all_small_cap()
    # await acquire_all_mid_cap()
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

def get_dataframes():
    global daily_small
    daily_small = Data("small", "daily")
    global one_hr_small
    one_hr_small = Data("small", "1hr")
    global five_min_small
    five_min_small = Data("small", "5min")
    global one_min_small
    one_min_small = Data("small", "1min")
    global mid_cap_data
    mid_cap_data = Data("mid", "daily")
    global one_hr_mid
    one_hr_mid = Data("mid", "1hr")
    global five_min_mid
    five_min_mid = Data("mid", "5min")
    global one_min_mid
    one_min_mid = Data("mid", "1min")

    global global_data
    global_data = {
        "daily_small": daily_small,
        "one_hr_small": one_hr_small,
        "five_min_small": five_min_small,
        "one_min_small": one_min_small
        # "mid": mid_cap_data
}

# Schedules

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


# API
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

@app.post("/changeStrategy")
async def change_strategy(model: Model):
    global_model["strat"] = model.strat
    global_model["cap"] = model.cap
    global_model["tf"] = model.tf
    return global_model


@app.get("/")
async def get_global_model():
    return global_model

@app.get("/screener")
async def screener():
    string = f"{global_model['tf']}_{global_model['cap']}"
    print(string)
    data = global_data[string]
    print(data)
    strat = global_model["strat"]
    strat = strategies[strat]
    filtered = data.filter(strat)
    return_all = filtered.return_all()
    return filtered.json_safe(return_all)

@app.get("/smallUptrends")
async def uptrending():
    string = f"{global_model['tf']}_{global_model['cap']}"
    print(string)
    data = global_data[string]
    print(data)
    strat = global_model["strat"]
    filtered = data.filter(strat)
    uptrends = filtered.uptrends()
    print(uptrends)
    return filtered.json_safe(uptrends)

