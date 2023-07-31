# Default
import datetime

# fastapi
from fastapi import FastAPI

# CORE
from wrabbit import rabbit_wrapper

# TASK

# app initialization
app = FastAPI()

# kafka initialization


@app.on_event("startup")
async def start_event():
    rabbit_wrapper.init()


@app.get("/")
async def say_hello():
    return {"hello": "world"}

@app.get("/hello")
async def create(scan_id:int):
    rabbit_wrapper.produce("hello", {"scan_id": scan_id})

@app.get("/stop_hello")
async def stop_hello(scan_id:int):
    rabbit_wrapper.produce("stop_hello", {"scan_id":scan_id})


