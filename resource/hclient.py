#!/usr/bin/python
#encoding:utf-8

import redis
import time
import traceback

rs = redis.Redis(host="localhost", port=5555)

n = 0
while True:
    key = "hedis:redis:fa|key-chenwenjiang%d" % n
    print key
    print rs.set(key, "valuedfasdfa\r\n..dfasdfadfa")
    time.sleep(1)
    #print rs.delete(key)
    print rs.exists(key)
    print rs.get(key)
    #time.sleep(12)
    n += 1




