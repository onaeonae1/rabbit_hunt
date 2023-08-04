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
async def create(scan_id: int):
    rabbit_wrapper.produce("start_hello", {"scan_id": scan_id})


@app.get("/stop_hello")
async def stop_hello(scan_id: int):
    rabbit_wrapper.produce("stop_hello", {"scan_id": scan_id})


@app.get("/start_crawler")
async def start_crawl(scan_id: int):
    rabbit_wrapper.produce("start_crawler", {
        "host": "192.168.7.77",
        "target_list": [
            {
                "port": 81,
                "state": "open",
                "protocol": "tcp",
                "service": "http",
            },
        ],
        "credentials": [],
        "scan_id": scan_id,
        "is_testing": True,
        "entry_paths": [],
    })


@app.get("/stop_crawler")
async def stop_crawl(scan_id: int):
    rabbit_wrapper.produce("stop_crawler", {"scan_id": scan_id})


@app.get("/start_scanner")
async def start_scanner(scan_id: int):
    rabbit_wrapper.produce("start_scanner", {
        "host": "192.168.7.77",
        "scan_id": scan_id,
        "port_list": "80, 81",
    })


@app.get("/stop_scanner")
async def stop_scanner(scan_id: int):
    rabbit_wrapper.produce("stop_scanner", {
        "scan_id": scan_id,
    })
