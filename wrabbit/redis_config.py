import redis


def connect_to_redis() -> redis.Redis:
    redis_connect = redis.Redis(
        host="redis",
        port=6379,
        db=10,
        charset="utf-8",
        decode_responses=True,
    )

    return redis_connect
