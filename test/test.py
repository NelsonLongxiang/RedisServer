#!/user/bin/env python3
# -*- coding: UTF-8 -*-
# @Time : 2024/6/15 下午5:02
# @Author : 龙翔
# @File    :test.py.py
# @Software: PyCharm

import os
import sys

# 将当前文件夹添加到环境变量
if os.path.basename(__file__) in ['run.py', 'main.py', '__main__.py']:
    if '.py' in __file__:
        sys.path.append(os.path.abspath(os.path.dirname(__file__)))
    else:
        sys.path.append(os.path.abspath(__file__))
from RedisServer_Queue import RedisServer

RedisServer.init()
