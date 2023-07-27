# Default
import datetime

# fastapi
from fastapi import FastAPI

# CORE
from wrabbit import test_wrapper

# TASK

# app initialization
app = FastAPI()

# kafka initialization


@app.on_event("startup")
async def start_event():
    test_wrapper.init()


@app.get("/")
async def say_hello():
    return {"hello": "world"}


@app.get("/hello")
async def create():
    test_wrapper.produce("hello", {"item": 123})


@app.get("/goodbye")
async def goodbye():
    test_wrapper.produce("goodbye", {"itemnnb": "123123"})
