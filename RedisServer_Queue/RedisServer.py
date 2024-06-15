#!/user/bin/env python3
# -*- coding: UTF-8 -*-
# @Time : 2024/6/6 上午6:16
# @Author : 龙翔
# @File    :RedisServer.py
# @Software: PyCharm
import json
import os
import sys
import time
import uuid

import redis

# 将当前文件夹添加到环境变量
if os.path.basename(__file__) in ['run.py', 'main.py', '__main__.py']:
    if '.py' in __file__:
        sys.path.append(os.path.abspath(os.path.dirname(__file__)))
    else:
        sys.path.append(os.path.abspath(__file__))

r = redis.Redis


def init(redis_obj=None):
    '''
    初始化redis
    :param redis_obj:
    :return:
    '''
    global r
    if redis_obj:
        r = redis_obj
    else:
        r = r(host='localhost', port=6379, db=0)


class RedisQueue:
    def __init__(self, topic=None):
        self.topic = topic

    def get(self):
        # 弹出指定数量的数据
        data = r.rpop(self.topic)
        return data if data else None

    def put(self, value):
        if isinstance(value, list):
            r.lpush(self.topic, *value)
        else:
            r.lpush(self.topic, *[value])

    def clear(self):
        r.delete(self.topic)

    def size(self):
        return r.llen(self.topic)

    def qsize(self):
        return self.size()

    def get_mul(self, num):
        return [self.get() for _ in range(num)]

    def re_data(self):
        ch_keys = r.keys(f"ack_{self.topic}_*")
        for key in ch_keys:
            data = r.get(key)
            if data:
                q = RedisQueue(self.topic)
                t, _data = json.loads(data)
                r.delete(key)
                q.put(_data)
        return len(ch_keys)

    def get_all(self):
        return r.lrange(self.topic, 0, -1)


class RedisMQ:
    def __init__(self):
        self.switch = 1

    def start_receive(self, topic, callback, count=-1):
        while self.switch:
            data = r.rpop(topic)
            if data:
                ch = RedisCh(topic, data)
                callback(ch, data)
                if count == 1:
                    return
                continue
            ch_keys = r.keys(f"ack_{topic}_*")
            for key in ch_keys:
                data = r.get(key)
                if data:
                    q = RedisQueue(topic)
                    t, _data = json.loads(data)
                    if time.time() - t > 10 * 60:
                        r.delete(key)
                        q.put(_data)
                        del q
            if len(ch_keys) == 0:
                time.sleep(10)
            time.sleep(1)

    def stop(self):
        self.switch = 0


class RedisCh:
    def __init__(self, topic, data):
        self.topic = topic
        self.id = uuid.uuid4()
        r.set(f"ack_{topic}_{self.id}", json.dumps([time.time(), data.decode()]))

    def basic_ack(self):
        r.delete(f"ack_{self.topic}_{self.id}")


if __name__ == '__main__':
    r.delete('test')
    # a = RedisQueue("test")
    # a.put(1)
    # print(type(a.get()))
    # r.set('test', 1)
    # print(r.get('test'), type(r.get('test').decode('utf8')))
    # RedisQueue.put(value=1, topic="test")
