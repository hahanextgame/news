# -*- coding: utf-8 -*-

# @Time :2018/11/23 8:51
# @File: gun.py
# @目的

import os
import gevent.monkey
gevent.monkey.patch_all()

import multiprocessing

#debug =True
bind = "0.0.0.0:80"

#workers = multiprocessing.cpu_count()*2 + 1
workers = 4
worker_class = "gevent"
threads = 20
preload_app = True
reload = True
x_forwarded_for_header = "X-FORWARDER-FOR"
