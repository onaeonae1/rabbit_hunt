from celery import Celery

celery_app = Celery(
    "crawler_app",
    broker="redis://redis:6379",
    backend="redis://redis:6379",
    include=["celery_worker"],
)

celery_app.conf.update(
    task_acks_late=True,
    broker_heartbeat=None,
    broker_transport_options={
        "fanout_prefix": True,
        "fanout_patterns": True,
        "visibility_timeout": 604800,
    },
)
