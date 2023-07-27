# default
import time

# typing
from typing import Dict


def hello_callback(message: Dict):
    print("==========HELLO===========")
    print(f"Received Message - > {message}")
    print("=======================")


def goodbye_callback(message: Dict):
    print("--------GOODBYE------------")
    print(f"Received Message -> {message}")
    print("---------------------------")
