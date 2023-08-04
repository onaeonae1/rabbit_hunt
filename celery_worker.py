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
        print("TASK FINISHED!")
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
        print("TASK EXIT!")
        redis_connect = connect_to_redis()
        redis_connect.publish(channel="task_channel", message=f"{task_id}")
        pass


@celery_app.task(base=CallbackTask)
def hello_task(scan_id: int):
    task_id = hello_task.request.id

    scan_id = int(scan_id)
    task_id = str(task_id)
    redis_connect = connect_to_redis()
    redis_connect.set(scan_id, task_id)
    redis_connect.close()

    print(f"HELLO -> [ID: {scan_id}] / TASK_ID: {task_id}")

    time.sleep(10)

    counter = 0
    while counter < 10:
        counter = counter+1
        print(f"hello counter from ID: {scan_id} -> {counter}")
        time.sleep(3)

    # cleanup


def start_task_hello(data: Dict):
    ret_value = None
    scan_id = data.get("scan_id")
    if scan_id is not None:
        try:
            redis_connect = connect_to_redis()
            executed_task_id = redis_connect.get(scan_id)
            if executed_task_id is None:
                ret_value = hello_task.delay(scan_id)
            else:
                print(
                    f"Task Already Executed -> {scan_id} || {executed_task_id}")
        except:
            pass
    return ret_value


@celery_app.task(base=CallbackTask)
def goodbye_task(message: Dict):
    print("Waiting in Progress")
    time.sleep(10)
    print("--------GOODBYE------------")
    print(f"Received Message -> {message}")
    print("---------------------------")
