# -*- coding: utf-8 -*-

# @Time :2018/12/7 18:34
# @File: RedisDataHandOut.py
# @目的

import redis
import json
import datetime
import ReSysApi2.configs as config

rdp = redis.ConnectionPool(host=config.redis_ip, port=config.redis_port, db=config.redis_db, password=config.redis_pass)
red = redis.StrictRedis(connection_pool=rdp)

# -------------------------------------------------------------------------------------------------------------------- #

def data_hand_out(data1):

    end = int(99999999999999999999999999)
    rdp = redis.ConnectionPool(host=config.redis_ip, port=config.redis_port, db=config.redis_db, password=config.redis_pass)
    red = redis.StrictRedis(connection_pool=rdp)

    showType = data1.get('showType', '')
    releaseTime = data1.get('releaseTime', '2019-01-04 14:41:02')
    ids = data1.get('contentId', 0)
    sourceType = data1.get('sourceType', '')


    # -- 置顶
    try:
        red.zremrangebyscore('data_top', ids, ids)
        if showType == 1:
            #print(data1, '\n')
            data2 = json.dumps(data1, ensure_ascii=False)
            print(ids, '\n')
            print(data2, '\n')
            red.zadd('data_top', ids, data2)
            b = red.zrangebyscore('data_top', ids, ids)
            print(b)
    except:
        pass


    # -- 快讯
    try:
        if showType==2:
            red.zadd('data_fast', ids, json.dumps(data1))

        now = datetime.datetime.now()
        before_30 = str(now - datetime.timedelta(minutes=5)) # 1/48
        all_data = red.zrangebyscore('data_fast', 0, end)
        def toDict(i):
            tmp = i.decode()
            res = json.loads(tmp)
            return res

        all_data1 = map(toDict, all_data)
        all_data = list(all_data1)

        times = {i.get("id"): i.get("kaTimes") for i in all_data}

        del_id = [k for k, v in times.items() if v < before_30]

        for i in range(len(del_id)):
            red.zremrangebyscore('data_fast', del_id[i], del_id[i])

        if showType == 2 and releaseTime < before_30:
            red.zadd('data_fast', ids, json.dumps(data1))
    except:
        print("error")



    # 排序后返回多少条
    try:
        if showType not in [1, 2]:
            if sourceType == 3:
                red.zadd('data_content_haikehao', ids, json.dumps(data1))
            else:
                red.zadd('data_content', ids, json.dumps(data1))
    except:
        pass



