# default
import time

# typing
from typing import Dict

# pip
from celery import Task

# custom
from celery_app import celery_app
from wrabbit.redis_config import connect_to_redis


class CallbackTask(Task):
    def on_success(self, retval, task_id, args, kwargs):
        """
        retval – The return value of the task.
        task_id – Unique id of the executed task.
        args – Original arguments for the executed task.
        kwargs – Original keyword arguments for the executed task.
        """
        redis_connect = connect_to_redis()
        redis_connect.publish(channel="task_channel", message=f"{task_id}")
        pass

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """
        exc – The exception raised by the task.
        task_id – Unique id of the failed task.
        args – Original arguments for the task that failed.
        kwargs – Original keyword arguments for the task that failed.
        """
        pass


@celery_app.task(base=CallbackTask)
def hello_task(message: Dict):
    task_id = hello_task.request.id
    # print(f"TASK_ID -> {task_id}")

    # print("WAITING in Progress")
    time.sleep(10)

    print(f"HELLO - > {message}")


@celery_app.task(base=CallbackTask)
def goodbye_task(message: Dict):
    print("Waiting in Progress")
    time.sleep(10)
    print("--------GOODBYE------------")
    print(f"Received Message -> {message}")
    print("---------------------------")
