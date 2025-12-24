import time

import requests

from .celery_config import celery_app


@celery_app.task
def add(x, y):
    return x + y


@celery_app.task
def mul(x, y):
    return x * y


@celery_app.task
def xsum(numbers):
    return sum(numbers)

@celery_app.task
def call_http(url):
    return requests.get(url).text