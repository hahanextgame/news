# -*- coding: utf-8 -*-

# @Time :2019/1/7 11:32
# @File: clearnRedis.py
# @目的: 定时清理redis中的数据

'''
    在晚上时候清理一遍数据
'''

import datetime
import schedule
import time

import redis
import json
import pandas as pd
import ReSysApi2.configs as config


def clean_job():
    '''
    :return:
    '''
    end = int(99999999999999999999999999)
    rdp = redis.ConnectionPool(host=config.redis_ip, port=config.redis_port, db=config.redis_db, password=config.redis_pass)
    red = redis.StrictRedis(connection_pool=rdp)

    all_data = red.zrangebyscore('data_house', 0, end)

    def toDict(i):
        tmp = i.decode()
        res = json.loads(tmp)
        return res

    all_data1 = map(toDict, all_data)
    all_data = list(all_data1)
    user_pd = pd.DataFrame(all_data)

    try:
        now = datetime.datetime.now()
        day22 = str(now - datetime.timedelta(22))
        del_house = user_pd[user_pd.releaseTime < day22]

        del_house_id = del_house.contentId.tolist()
        for i in range(len(del_house_id)):
            red.zremrangebyscore('data_house', del_house_id[i], del_house_id[i])
    except:
        pass

    try:
        now = datetime.datetime.now()
        day7 = str(now - datetime.timedelta(2))
        del_house = user_pd[user_pd.releaseTime < day7]

        del_house_id = del_house.contentId.tolist()
        for i in range(len(del_house_id)):
            red.zremrangebyscore('data_content', del_house_id[i], del_house_id[i])


        # ---
        # day7 = str(now - datetime.timedelta(25))
        day7 = str(now - datetime.timedelta(4))
        del_house = user_pd[user_pd.releaseTime < day7]

        del_house_id = del_house.contentId.tolist()
        for i in range(len(del_house_id)):
            red.zremrangebyscore('data_content_haikehao', del_house_id[i], del_house_id[i])
    except:
        pass



schedule.every().day.at("10:30").do(clean_job)


while True:
    schedule.run_pending()
    time.sleep(1)