#!/user/bin/env python3
# -*- coding: UTF-8 -*-
# @Time : 2024/6/6 上午6:16
# @Author : 龙翔
# @File    :RedisServer.py
# @Software: PyCharm
import pickle
import time
import typing
import uuid
from typing import Callable

import redis
from loguru import logger


class RedisQueue:
    def __init__(self, r: redis.Redis, topic: str, ack_timeout=600):
        self.topic = topic
        self.r = r
        self.ack_timeout = ack_timeout

    def get(self):
        try:
            data = self.r.rpop(self.topic)
            if data:
                return RedisCh(self.r, self.topic, data)
            return None
        except redis.RedisError as e:
            logger.error(f"Redis error in get: {e}")
            return None

    def get_mul(self, num):
        return [self.get() for _ in range(num) if self.get() is not None]

    def put(self, value:typing.Any):
        try:
            if not isinstance(value, list):
                value = [value]
            value = [pickle.dumps((time.time(), v)) for v in value]
            self.r.lpush(self.topic, *value)
        except redis.RedisError as e:
            logger.error(f"Redis error in put: {e}")

    def clear(self):
        try:
            self.r.delete(self.topic)
        except redis.RedisError as e:
            logger.error(f"Redis error in clear: {e}")

    def size(self):
        try:
            return self.r.llen(self.topic)
        except redis.RedisError as e:
            logger.error(f"Redis error in size: {e}")
            return 0

    def qsize(self):
        return self.size()

    def re_data(self,init=False):
        # 使用SCAN替代KEYS以提高性能
        cursor = 0
        ack_keys = []
        while True:
            cursor, keys = self.r.scan(cursor, match=f"queue_ack/{self.topic}/*")
            ack_keys.extend(keys)
            if cursor == 0:
                break

        for key in ack_keys:
            data = self.r.get(key)
            if data:
                t, _data = pickle.loads(data)
                if time.time() - t > self.ack_timeout or init:
                    self.r.delete(key)
                    self.put(_data)

    def get_all(self):
        try:
            return self.r.lrange(self.topic, 0, -1)
        except redis.RedisError as e:
            logger.error(f"Redis error in get_all: {e}")
            return []


class RedisMQ:
    def __init__(self):
        self.switch = 1

    def start_receive(self, r: redis.Redis, topic: str, callback: Callable, count=-1):
        queue_obj = RedisQueue(r, topic)
        queue_obj.re_data()

        while self.switch:
            try:
                if queue_obj.qsize() == 0:
                    queue_obj.re_data()
                    time.sleep(5)
                    continue

                ch = queue_obj.get()
                if ch:
                    callback(ch, ch.data)
                    if count == 1:
                        return
                    continue
                queue_obj.re_data()

            except redis.RedisError as e:
                logger.error(f"Redis error in start_receive: {e}")
                time.sleep(5)

    def stop(self):
        self.switch = 0


class RedisCh:
    def __init__(self, r: redis.Redis, topic: str, data: bytes):
        self.r = r
        self.topic = topic
        self.id = uuid.uuid4()
        self.time, self.data = pickle.loads(data)
        try:
            r.set(f"queue_ack/{topic}/{self.id}", data, ex=60 * 60 * 24 * 7)
        except redis.RedisError as e:
            logger.error(f"Redis error in RedisCh init: {e}")

    def basic_ack(self):
        try:
            self.r.delete(f"ack_{self.topic}_{self.id}")
        except redis.RedisError as e:
            logger.error(f"Redis error in basic_ack: {e}")