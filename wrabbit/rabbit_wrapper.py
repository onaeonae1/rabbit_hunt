# default
from json import dumps, loads
import logging
from threading import Thread, RLock, Lock
from functools import partial
from datetime import datetime
import traceback
import time

# typing
from typing import Dict, Callable

# pip
import pika
from pika.spec import Basic, BasicProperties, PERSISTENT_DELIVERY_MODE
from pika.channel import Channel
from pika.exceptions import (
    ConnectionClosedByBroker,
    AMQPChannelError,
    AMQPConnectionError,
)
from pika.adapters.blocking_connection import BlockingConnection

# custom
from wrabbit import redis_config

# tasks
from celery_worker import hello_task, goodbye_task

connection_lock = Lock()


class RabbitConfig:
    # print 옵션
    verbose = True

    # 연결 옵션
    host = "192.168.7.166"
    port = 5672
    username = "guest"
    password = "guest"
    topics = ["hello", "goodbye"]

    # 내부 사용 목적 -> may be replaced with redis..?
    store = {}

    def receiver_callback(
        self,
        ch: Channel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: str,
        callback: Callable,
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
        res = callback(topic, data)
        if res is not None:
            task_id = str(res)
            delivery_key = f"{task_id}"
            connect = redis_config.connect_to_redis()
            connect.set(delivery_key, method.delivery_tag)
            connect.close()
        else:
            self.ack_callback(ch, method.delivery_tag)

    def ack_callback(self, ch: Channel, delivery_tag: str):
        delivery_tag = int(delivery_tag)
        if self.verbose:
            print(f"[O] ACK  -> {delivery_tag}")

        if ch.is_open:
            ch.basic_ack(delivery_tag=delivery_tag, multiple=False)
        else:
            print(f"[!] ACK Fail: 채널 닫힘 -> {delivery_tag}")

    def controller_callback(self, topic: str, data: Dict):
        # 토픽에 맞춰서 구현하신 핸들러를 필요에 따라 import해 사용하시면 됩니다
        if topic in self.topics:
            if topic == "hello":
                return hello_task.delay(data)
            elif topic == "goodbye":
                return goodbye_task.delay(data)
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

    channel: Channel = None
    producer_channel: Channel = None

    def __init__(self) -> None:
        super().__init__()
        self.__connect()

    def __del__(self):
        print("연결 종료")
        self.__close()

    def __connect(self, producer: bool = False):
        if not producer:
            if self.connection is None:
                self.connection = BlockingConnection(
                    pika.URLParameters(
                        f"amqp://{self.username}:{self.password}@{self.host}:{self.port}/"
                    )
                )

        else:
            if self.producer_connection is None:
                self.producer_connection = BlockingConnection(
                    pika.URLParameters(
                        f"amqp://{self.username}:{self.password}@{self.host}:{self.port}/"
                    )
                )

    def __channel(self, producer: bool = False):
        self.__connect(producer)
        if not producer:
            if self.channel is None:
                self.channel = self.connection.channel()
        else:
            if self.producer_channel is None:
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
                self.channel.exchange_declare(exchange="default", exchange_type="topic")
                on_message_callback = partial(
                    self.receiver_callback, callback=self.controller_callback
                )

                for topic in self.topics:
                    self.channel.queue_declare(queue=topic, durable=True)
                    self.channel.basic_consume(
                        queue=topic,
                        on_message_callback=on_message_callback,
                        auto_ack=False,
                    )

                # prefetch_count 가 늘어날수록 "메시지 처리 완료 전"에 consume 되어 buffer에 저장되는 데이터 증가함
                self.channel.basic_qos(prefetch_count=2)
                self.channel.start_consuming()

            except ConnectionClosedByBroker:
                print("[!] Connection Closed By Broker -> Recovery Disabled")
                break
            except AMQPChannelError:
                print("[!] Channel Error -> Recovery Disabled ")
                break
            except AMQPConnectionError:
                print("[?] AMQP Connection Error -> Start Recovery!")
                continue

    def __redis_subscribe(self):
        redis_connect = redis_config.connect_to_redis()
        pubsub = redis_connect.pubsub()
        pubsub.subscribe(["task_channel"])

        while True:
            # print("Waiting Message")
            res: Dict = pubsub.get_message()
            if res is not None:
                task_id = res.get("data")
                if task_id is not None:
                    delivery_tag = redis_connect.get(task_id)
                    if delivery_tag is not None:
                        try:
                            self.ack_callback(self.channel, delivery_tag)
                        except:
                            time.sleep(10)
                            self.ack_callback(self.channel, delivery_tag)

    def consume(self):
        thread = Thread(target=self.__subscribe, args=())
        thread.daemon = True
        thread.start()

    def redis_watch(self):
        thread = Thread(target=self.__redis_subscribe, args=())
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
        channel = self.producer_connection.channel()
        channel.basic_publish(
            exchange="",
            routing_key=topic,
            body=formatted_data,
            properties=pika.BasicProperties(delivery_mode=PERSISTENT_DELIVERY_MODE),
        )
        print(f"message published with routing key -> {topic}")


# 싱글톤 및 lock 관련 내용
__cache_key = "rabbit_default"
__cache_wrappers = {}
__cache_wrappers_lock = RLock()


# 로깅
logger = logging.getLogger("rabbitmq_wrapper")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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
            wrapper.redis_watch()
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
