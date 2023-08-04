# default
from json import dumps, loads
import logging
from threading import Thread, RLock, Lock
from functools import partial
from datetime import datetime
import traceback
from concurrent.futures import ThreadPoolExecutor

# typing
from typing import Dict, Callable

# pip
import pika
from pika.spec import Basic, BasicProperties, PERSISTENT_DELIVERY_MODE
from pika.exceptions import (
    ConnectionClosedByBroker,
    AMQPChannelError,
    AMQPConnectionError,
)
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel

# custom
from wrabbit import redis_config

# tasks
from celery_app import celery_app
from celery_worker import start_task_hello

connection_lock = Lock()
execution_pool = ThreadPoolExecutor(max_workers=10)


def redis_subscribe(original_tag: int, ch: BlockingChannel, callback: Callable):
    # watch redis for some amout of time
    # if there's no corresponding data raise Error
    redis_connect = redis_config.connect_to_redis()
    pubsub = redis_connect.pubsub()
    pubsub.subscribe(["task_channel"])

    delivery_tag = None
    print(f"Start Watching Tag -> {original_tag}")
    while True:
        # print("Waiting Message")
        res: Dict = pubsub.get_message()
        if res is not None:
            print(res)
            task_id = res.get("data")
            if task_id is not None:
                delivery_tag = redis_connect.get(task_id)
                if delivery_tag is not None:
                    delivery_tag = int(delivery_tag)
                    if delivery_tag == original_tag:
                        print(
                            f"data -> {res} || delivery_tag -> {delivery_tag}")
                        redis_connect.delete(task_id)
                        pubsub.close()
                        ch.connection.add_callback_threadsafe(callback)
                        break

    return delivery_tag


def redis_stop_task(data: Dict):
    scan_id = data.get("scan_id")
    is_valid = False
    if scan_id is not None:
        redis_connect = redis_config.connect_to_redis()
        task_id = redis_connect.get(scan_id)
        if task_id is not None:
            is_valid = True
            print(f"DELETE TASK_ID: {task_id} for scan_id: {scan_id}")
            # celery_app.control.revoke(task_id, terminal=True, signal="SIGTERM")
            try:
                celery_app.control.terminate(task_id, signal="SIGTERM")
                celery_app.control.revoke(
                    task_id, signal="SIGKILL", terminate=True)
            except:
                pass
            redis_connect.delete(scan_id)
            # 종료 ACK 를 위한 몸부림
            print(f"Manual Termination for task_id -> {task_id}")
            redis_connect.publish(channel="task_channel", message=f"{task_id}")
            redis_connect.close()
    return is_valid


class RabbitConfig:
    # print 옵션
    verbose = True

    # 연결 옵션
    host = "192.168.7.166"
    port = 5672
    username = "guest"
    password = "guest"

    # topic : callback(args=data) 형태의 맵 정보를 가지고 처리가능
    topic_mapping = {
        "start_hello": start_task_hello,
        "stop_hello": redis_stop_task,
    }

    # 내부 사용 목적 -> may be replaced with redis..?
    store = {}

    def receiver_callback(
        self,
        ch: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: str,
    ):
        """최초로 메시지를 수신, 포메팅 후 작업을 처리하는 calllback 을 호출, 이후 수동 ACK 를 호출

        Args:
            ch (_type_): 연결 채널
            method (_type_): _description_
            properties (_type_): _description_
            body (str): 실제로 수신된 str 형태의 데이터
            callback (Callable): RabbitConfig 내에 있는 controller_callback 호출
        """
        body = body.decode()
        message: Dict = loads(body)
        topic = message.get("topic", "")
        data = message.get("data", {})

        if self.verbose:
            print(f"[O] RECV [TOPIC: {topic}] -> {method}")

        # 비동기 작업 ID
        res = None
        task: Callable = self.topic_mapping.get(topic)
        res = task(data) if task is not None else None
        if res is not None:
            if res == 0:
                ch.basic_nack(method.delivery_tag)
            elif res == 1:
                self.ack_callback(ch, method.delivery_tag, origin="TASK_STOP")

            else:
                task_id = str(res)
                delivery_key = f"{task_id}"
                redis_connect = redis_config.connect_to_redis()
                redis_connect.set(delivery_key, method.delivery_tag)
                redis_connect.close()

                # 미리 callback 을 선언
                ack_message_callback = partial(
                    self.ack_callback,
                    ch=ch,
                    delivery_tag=method.delivery_tag,
                    origin="CELERY_TASK",
                )
                execution_pool.submit(
                    redis_subscribe, method.delivery_tag, ch, ack_message_callback
                )

        else:
            self.ack_callback(
                ch,
                method.delivery_tag,
            )

    def ack_callback(
        self, ch: BlockingChannel, delivery_tag: str, origin: str = "DEFAULT"
    ):
        delivery_tag = int(delivery_tag)
        if self.verbose:
            print(f"[O] <{origin}> ACK -> {delivery_tag}")

        if ch.is_open:
            ch.basic_ack(delivery_tag=delivery_tag, multiple=False)
        else:
            print(f"[!] ACK Fail: 채널 닫힘 -> {delivery_tag}")

    def controller_callback(self, topic: str, data: Dict):
        # 토픽에 맞춰서 구현하신 핸들러를 필요에 따라 import해 사용하시면 됩니다
        task: Callable = self.topic_mapping.get(topic)
        if task is not None:
            return task(data)
        else:
            print(f"Wrabbit ERROR: Undefined Topic -> {topic}")
            return None


class RabbitWrapper(RabbitConfig):
    """RabbitConfig 를 상속, 실제 연결 생성 및 Produce/Consume 처리하는 CLI로 동작

    Args:
        RabbitConfig (_type_): _description_
    """

    connection: BlockingConnection = None
    producer_connection: BlockingConnection = None

    channel: BlockingChannel = None
    producer_channel: BlockingChannel = None

    def __init__(self) -> None:
        super().__init__()
        self.__connect()

    def __del__(self):
        print("연결 종료")
        self.__close()

    def __connect(self, producer: bool = False):
        if not producer:
            if self.connection is None:
                print(f"Make New Connection(producer: {producer})")
                self.connection = BlockingConnection(
                    pika.URLParameters(
                        f"amqp://{self.username}:{self.password}@{self.host}:{self.port}/"
                    )
                )

        else:
            if self.producer_connection is None:
                print(f"Make New Connection(producer: {producer})")
                self.producer_connection = BlockingConnection(
                    pika.URLParameters(
                        f"amqp://{self.username}:{self.password}@{self.host}:{self.port}/"
                    )
                )

    def __channel(self, producer: bool = False):
        self.__connect(producer)
        if not producer:
            if self.channel is None:
                print(f"Make New Channel(producer: {producer})")
                self.channel = self.connection.channel()
        else:
            if self.producer_channel is None:
                print(f"Make New Channel(producer: {producer})")
                self.producer_channel = self.producer_connection.channel()

    def __close_channel(self):
        if self.channel is not None:
            self.channel.close()
        if self.producer_channel is not None:
            self.producer_channel.close()

    def __close(self):
        if self.connection is not None:
            self.connection.close()
        if self.producer_connection is not None:
            self.producer_connection.close()

    def __subscribe(self):
        self.__connect()
        while True:
            try:
                self.__channel()
                self.channel.exchange_declare(
                    exchange="default", exchange_type="topic")
                for topic in list(self.topic_mapping.keys()):
                    self.channel.queue_declare(queue=topic, durable=True)
                    self.channel.basic_consume(
                        queue=topic,
                        on_message_callback=self.receiver_callback,
                        auto_ack=False,
                    )

                # prefetch_count 가 늘어날수록 "메시지 처리 완료 전"에 consume 되어 buffer에 저장되는 데이터 증가함
                self.channel.basic_qos(prefetch_count=2)
                self.channel.start_consuming()

            except ConnectionClosedByBroker:
                print("[!] Connection Closed By Broker -> Recovery Disabled")
                break
            except AMQPChannelError as e:
                print(f"[!] Channel Error [{e}]-> fuckoff!")
                break
            except AMQPConnectionError:
                print("[?] AMQP Connection Error -> Start Recovery!")
                continue

    def consume(self):
        thread = Thread(target=self.__subscribe, args=())
        thread.daemon = True
        thread.start()

    def produce(self, topic: str, data: Dict):
        # time setting
        now = datetime.now()
        timestr = now.strftime("%Y-%m-%d %H:%M:%S")
        data["timestamp"] = timestr

        ret_message = {}
        ret_message["topic"] = topic
        ret_message["data"] = data
        print(ret_message)

        formatted_data = dumps(ret_message)
        self.__connect(producer=True)
        self.__channel(producer=True)
        channel = self.producer_connection.channel()
        channel.basic_publish(
            exchange="",
            routing_key=topic,
            body=formatted_data,
            properties=pika.BasicProperties(
                delivery_mode=PERSISTENT_DELIVERY_MODE),
        )
        print(f"message published with routing key -> {topic}")


# 싱글톤 및 lock 관련 내용
__cache_key = "rabbit_default"
__cache_wrappers = {}
__cache_wrappers_lock = RLock()


# 로깅
logger = logging.getLogger("rabbitmq_wrapper")
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


# 생성 코드
def init():
    with __cache_wrappers_lock:
        if __cache_key in __cache_wrappers:
            logger.warning(
                "A wrapper is already running, try to stop it and start the new one!"
            )
            __cache_wrappers[__cache_key].stop()
            del __cache_wrappers[__cache_key]
        try:
            wrapper = RabbitWrapper()
            __cache_wrappers[__cache_key] = wrapper
            wrapper.consume()
            logger.info("A wrapper has been started.")
            return wrapper
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return None


# CLI 획득 -> APP 측에서 호출해서 사용 가능하도록
def get_wrapper():
    with __cache_wrappers_lock:
        if __cache_key in __cache_wrappers:
            return __cache_wrappers[__cache_key]
        else:
            return None


# 토픽 생성 인터페이스
def produce(topic, data: Dict):
    cli = get_wrapper()
    if cli is None:
        raise Exception("Rabbit Wrapper has not initialized.")
    return cli.produce(topic, data)
