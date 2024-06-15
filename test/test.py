#!/user/bin/env python3
# -*- coding: UTF-8 -*-
# @Time : 2024/6/15 下午5:02
# @Author : 龙翔
# @File    :test.py.py
# @Software: PyCharm

import os
import sys

import redis

# 将当前文件夹添加到环境变量
if os.path.basename(__file__) in ['run.py', 'main.py', '__main__.py']:
    if '.py' in __file__:
        sys.path.append(os.path.abspath(os.path.dirname(__file__)))
    else:
        sys.path.append(os.path.abspath(__file__))

# 导入模块
from RedisServer_Queue import RedisServer

# 初始化Redis服务
# 默认使用本地Redis服务 6379端口 如果您使用的是远程Redis服务 请修改将实例化后的r对象传入RedisServer.init(r)
# 使用本地Redis服务端口是6379 默认db是0 则不需要传入参数
'''
r = redis.Redis(host='localhost', port=6379, db=0)
'''
r = redis.Redis(host='localhost', port=6379, db=0)

RedisServer.init(redis_obj=r)


def hello_world():
    '''
    get 数据返回类型为bytes类型
    :return:
    '''
    msg_queue = RedisServer.RedisQueue(topic='msg_queue')
    if msg_queue.qsize() == 0:
        msg_queue.put('hello world')

    print(msg_queue.get())


def ack_test():
    '''
    ack_test
    :return:
    '''
    msg_queue = RedisServer.RedisQueue(topic='msg_queue')
    print("重置数据,初始化时运行,返回值为重置数据量！", msg_queue.re_data())
    msg_queue.put('hello world')

    class ThreadAck(RedisServer.RedisQueue, RedisServer.RedisMQ):
        def __init__(self, topic):
            RedisServer.RedisQueue.__init__(self, topic)
            RedisServer.RedisMQ.__init__(self)
            self.ch = None

        def run(self):
            '''count 默认为-1；当count=1时，表示只获取一个数据，当count>1时，表示获取count个数据
                根据需要自行修改
            '''
            self.start_receive(topic=self.topic, callback=self.callback)

        def callback(self, ch, body):
            self.ch = ch
            self.work(body)
            print("剩余队列长度：", self.qsize())

        def work(self, data):
            if data:
                print(data)
                '''
                ack 注释 可以通过re_data()方法重新将数据放入队列
                '''
                self.ch.basic_ack()

    thread_ack = ThreadAck(topic='msg_queue')
    thread_ack.run()


if __name__ == '__main__':
    # hello_world()
    ack_test()
