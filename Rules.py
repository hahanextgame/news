# -*- coding: utf-8 -*-

# @Time :2018/11/19 13:49
# @File: ReLogic.py
# @目的



import datetime
import requests
import math
import pandas as pd
import redis
import json
import time
from elasticsearch import Elasticsearch
from math import radians, cos, sin, asin, sqrt

import ReSysApi2.configs as config
from ReSysApi2.outFile import result_es_out
from ReSysApi2.until import black_query


def now_cut(now, readed):
    '''
    新闻距今的时间表
    :param now:
    :param readed:
    :return:
    '''
    try:
        temp = now - readed
        return temp.value
    except:
        return int(999999999)


def str_to_time(x):
    tmp = datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
    return tmp


def haversine(lon1, lat1, lon2, lat2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    lon1 = float(lon1)
    lat1 = float(lat1)
    lon2 = float(lon2)
    lat2 = float(lat2)

    if lon1 == 0 or lat1 == 0:
        return float(999999999)

    if lon2 == 0 or lon2 ==[]:
        return float(999999999)

    if lat2 == 0 or lat2 ==[]:
        return float(999999999)


    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    return c * r * 1000

#
def score_weight_three(time, distance, pri, w=[1, 1, 2]):
    '''
    2.5 : 1 : 1
    :param time:
    :param distance:
    :param pri:
    :param w:
    :return:
    '''
    if pd.isna(pri): pri = 0
    if pd.isna(distance): distance = 0
    if pd.isna(time): time = 0

    x, y, z = float(time), float(distance), float(pri)
    #result = w[0]*x + w[1]*y + w[2]*z
    x = math.exp(5*x-4)
    z = math.exp(5*z-5)
    result = w[0] * x + w[1] * y + w[2] * z
    return result


def score_weight(time, pri, w=[2, 2]):
    '''
    2.5 : 1
    :param time:
    :param pri:
    :param w:
    :return:
    '''
    if pd.isna(pri): pri = 0
    if pd.isna(time): time = 0

    x, y = float(time), float(pri)
    x = math.exp(5*x-4)
    y = math.exp(5*y-5)
    result = w[0] * x + w[1] * y
    return result


def get_id(lon, lat, df_content, rule, change_weight=False):
    '''
    :param lon:
    :param lat:
    :param df_content:
    :param rule:
    :param change_weight:
    :return:
    '''
    if df_content.__len__() == 0:
        return df_content
    df_content.type = df_content.type.map(str)
    df_content.contentType = df_content.contentType.map(str)
    df_content.loc[df_content.type == '12', 'contentType'] = 6

    # 用户的精度还有维度
    user_lon = float(lon)
    user_lat = float(lat)

    df_content.loc[df_content.contentType == "3", 'title'] = df_content.loc[df_content.contentType == "3", 'contentId']
    df_content = df_content.drop_duplicates(['contentId'])
    df_content = df_content.drop_duplicates(['title'])


    df_content.contentType =df_content.contentType.replace('', 0)
    df_content.contentType = df_content.contentType.fillna(int(0))

    df_content.priority = df_content.priority.replace('', 0)
    df_content.priority = df_content.priority.fillna(int(0))



    df_content = df_content[-((df_content.contentType == "3") & (df_content.priority <= 0))]
    df_content = df_content[-((df_content.contentType == "4") & (df_content.priority <= 0))]



    df_content = df_content.sort_values(['releaseTime'], ascending=[False])

    if df_content.__len__() == 0:
        return df_content

    df_content.loc[:, 'user_lon'] = user_lon
    df_content.loc[:, 'user_lat'] = user_lat

    df_content.longitude = df_content.longitude.replace('', 0)
    df_content.latitude = df_content.latitude.replace('', 0)

    df_content.loc[:, "distance"] = df_content.apply(lambda x: haversine(x.user_lon, x.user_lat, x.longitude, x.latitude), axis=1)
    df_content.loc[:, 'distance'] = df_content.distance.rank(ascending=False, method="first")  # 小的大

    df_content.loc[:, 'publishTime'] = df_content.releaseTime.fillna(df_content.releaseTime[df_content.releaseTime != ""].min())
    # df_content.publishTime = df_content.publishTime.replace('', df_content.releaseTime[df_content.releaseTime != ""].min())

    # df_content.priority = df_content.priority.replace('', 0)
    df_content.loc[:, 'priority'] = df_content.priority.fillna(df_content.priority.min())
    df_content.loc[:, 'time'] = df_content.publishTime.rank()


    # df_content.loc[:, 'priority'] = df_content.priority.fillna(int(0))
    df_content.loc[:, 'time'] = df_content.time.fillna(int(0))
    df_content.loc[:, 'distance'] = df_content.distance.fillna(int(0))

    df_content.loc[:, 'priority_tmp'] = (df_content.priority - df_content.priority.min()) / (
                df_content.priority.max() - df_content.priority.min())
    df_content.loc[:, 'distance'] = (df_content.distance - df_content.distance.min()) / (
                df_content.distance.max() - df_content.distance.min())
    df_content.loc[:, 'time'] = (df_content.time - df_content.time.min()) / (df_content.time.max() - df_content.time.min())


    if change_weight:
        df_content.loc[:, 'score'] = df_content.apply(lambda x: score_weight_three(x.time, x.distance, x.priority_tmp, w=[1, 10, 1]), axis=1)
    else:
        df_content.loc[:, 'score'] = df_content.apply(lambda x: score_weight_three(x.time, x.distance, x.priority_tmp), axis=1)
    df_content = df_content.sort_values(['score', 'contentId'], ascending=False)

    df_content = df_content.drop(['priority_tmp'], axis=1)
    df_content.contentType = df_content.contentType.map(str)


    ids = list(rule.keys())
    a = df_content[df_content.contentType == ids[0]][: int(rule.get(ids[0]))]
    for i in range(1, len(ids)):
        b = df_content[df_content.contentType == ids[i]][:int(rule.get(ids[i]))]
        a = pd.concat([a, b])

    result = a.sort_values(['score'], ascending=[False])
    return result



def get_country(country_code, df_content, rule):
    '''
    '''

    country_code = str(country_code)
    df_content.type = df_content.type.map(str)
    df_content.loc[df_content.type == '12', 'contentType'] = 6

    # --筛选
    df_content.countryCode = df_content.countryCode.map(str)
    df_content = df_content[df_content.countryCode == country_code]

    df_content = df_content.drop_duplicates(['contentId'])
    df_content = df_content.drop_duplicates(['title'])

    if df_content.__len__() == 0:
        return df_content

    df_content.contentType = df_content.contentType.map(str)

    df_content.contentType =df_content.contentType.replace('', 0)
    df_content.contentType = df_content.contentType.fillna(int(0))

    df_content.priority = df_content.priority.replace('', 0)
    df_content.priority = df_content.priority.fillna(int(0))


    df_content = df_content[-((df_content.contentType == "3") & (df_content.priority <= 0))]
    df_content.loc[df_content.contentType == "3", 'priority'] = df_content.loc[
                                                                    df_content.contentType == 3, 'priority'] * 3

    df_content = df_content.sort_values(['releaseTime'], ascending=[False])
    df_content = df_content[-(df_content.releaseTime == "")]

    # -- 空值填充
    df_content['publishTime'] = df_content.releaseTime.fillna(df_content.releaseTime.min())
    df_content.publishTime = df_content.publishTime.replace('', df_content.publishTime.min())

    df_content.priority = df_content.priority.replace('', 0)
    df_content['priority'] = df_content.priority.fillna(df_content.priority.min())
    df_content['time'] = df_content.releaseTime.rank(ascending=False)

    df_content['priority'] = df_content.priority.fillna(int(0))
    df_content['time'] = df_content.time.fillna(int(0))

    # -- 标准化, 相关的值均不能返回
    df_content['time'] = (df_content.time - df_content.time.min()) / (df_content.time.max() - df_content.time.min())
    df_content['priority_tmp'] = (df_content.priority - df_content.priority.min()) / (
                df_content.priority.max() - df_content.priority.min())

    # -- 加权评分
    df_content['score'] = df_content.apply(lambda x: score_weight(x.time, x.priority_tmp), axis=1)
    df_content = df_content.sort_values(['score', 'contentId'], ascending=False) # 排序

    df_content = df_content.drop(['priority_tmp'], axis=1)

    df_content.contentType = df_content.contentType.map(str)

    # --规则抽取
    if isinstance(df_content.contentType.tolist()[0], str):
        # --规则抽取
        ids = list(rule.keys())
        a = df_content[df_content.contentType == ids[0]][: int(rule.get(ids[0]))]
        for i in range(1, len(ids)):
            b = df_content[df_content.contentType == ids[i]][:int(rule.get(ids[i]))]
            a = pd.concat([a, b])

    else:
        # --规则抽取
        ids = list(rule.keys())
        a = df_content[df_content.contentType == int(ids[0])][:int(rule.get(ids[0]))]
        for i in range(1, len(ids)):
            b = df_content[df_content.contentType == int(ids[i])][:int(rule.get(ids[0]))]
            a = pd.concat([a, b])

    result = a.sort_values(['score'], ascending=[False])
    return result


def get_es_data(user_id, imei, size = 100):

    from elasticsearch import Elasticsearch
    es = Elasticsearch(config.es_ip, port=config.es_port)

    if user_id:
        xids = str(imei) + "_" + str(user_id)
    else:
        xids = str(imei)

    # 没有查询到的
    query = {
        "query": {
            "bool": {
                "must": [{
                    "term": {
                        "hisid": {
                            "value": xids}
                    }
                }]
            }
        }
        , "size": size
        , "sort": [
            {
                "recommTimeStamp": {
                    "order": "desc"
                }
            }
        ]
    }
    indexs = "recommendhist"
    doc_type1 = "test"
    test = es.search(index=indexs, doc_type=doc_type1, body=query) # 没有查询到的时候产生[]

    data_source = test['hits']['hits']

    def get_hist_content(x):
        tmp = x['_source']
        ids = tmp.get("contentId")
        return ids

    hist_recom = list(map(get_hist_content, data_source))
    return hist_recom


def get_content(name='data_content'):

    rdp = redis.ConnectionPool(host=config.redis_ip, port=config.redis_port, db=config.redis_db, password=config.redis_pass)
    red = redis.StrictRedis(connection_pool=rdp)
    t = 9999999999999999999999999999999
    all_data = red.zrangebyscore(name, 0, t)

    def toDict(i):
        tmp = i.decode()
        res = json.loads(tmp)
        return res

    all_data1 = map(toDict, all_data)
    all_data = list(all_data1)
    user_pd = pd.DataFrame(all_data)

    return user_pd


def cut_hist(user_id, imei, df_content, size=100):

    hist_recom = get_es_data(user_id, imei, size)
    a = df_content.contentId.values.tolist()
    cond1 = [i not in hist_recom for i in a]
    df_content = df_content[cond1]
    return df_content


def put_es(memberId, imei, df_content):


    if df_content.__len__() == 0:
        return

    histd = df_content

    createTime = histd.releaseTime.tolist()
    contentId = histd.contentId.tolist()
    sourceType = histd.sourceType.tolist()

    contentType = histd.contentType.map(str).tolist()
    es = Elasticsearch(config.es_ip, port=config.es_port)

    plateType = 1
    # for循环开始
    ## 确定hisid
    for i in range(contentId.__len__()):
        timestamp = int(time.time())

        if memberId == None:
            hisid = imei
        else:
            hisid = str(imei) + "_" + str(memberId)

        #_xid = str(plateType) + hisid  # ID
        _xid = hisid  # ID--查询使用
        ids =_xid  # id的中结构
        # 构造相关的代码
        print('--------------------------------')

        nows = str(datetime.datetime.now())[:19]

        if createTime[i] != '':
            cr_Time = createTime[i]
        else:
            cr_Time = nows

        hist = {
            "recommTimeStamp": timestamp,
            "plateType": int(sourceType[i]),
            "createTime": cr_Time,
            "hisid": hisid,  # 机器码_用户码
            "contentId":  contentId[i],
            "_xid": contentType[i],                        # 这个变成contentType
            "imei": imei,
            "type": 1,
            "memberId": memberId
        }
        # print(hist)
        #print(i)
        # print(createTime[i])
        ids = ids + "_" + str(contentId[i])
        es.index(index="recommendhist", id=ids, body=json.dumps(hist), doc_type="test")



def get_data_streamlet(topics, n=2):
    try:
        rdp = redis.ConnectionPool(host=config.redis_ip, port=config.redis_port, db=config.redis_db, password=config.redis_pass)
        red = redis.StrictRedis(connection_pool=rdp)
        data_fast = red.zrangebyscore(topics, 0, 9999999999999999999999999)

        def toDict(i):
            tmp = i.decode()
            res = json.loads(tmp)
            return res

        all_data1 = map(toDict, data_fast)
        all_data = list(all_data1)
        all_data.sort(key=lambda obj: (obj.get('kaTimes')), reverse=True)
        output = all_data[:n]
        return output
    except:
        return []


def put_es_list(memberId, imei, redis_list):

    lens = len(redis_list)
    createTime = [str(datetime.datetime.now())[:19]]*lens

    contentId = [i['id'] for i in redis_list]
    es = Elasticsearch(config.es_ip, port=config.es_port)

    plateType = 1
    # for循环开始
    ## 确定hisid
    for i in range(contentId.__len__()):
        timestamp = int(time.time())

        if memberId == None:
            hisid = imei
        else:
            hisid = str(imei) + "_" + str(memberId)
        _xid = hisid  # ID--查询使用
        ids =_xid  # id的中结构

        hist = {
            "recommTimeStamp": timestamp,
            "plateType": 0,  
            "createTime": createTime,
            "hisid": hisid, 
            "contentId": contentId[i],
            "_xid": "999",
            "imei": imei,
            "type": 2,
            "memberId": memberId
        }
        ids = ids + "_" + str(contentId[i])
        es.index(index="recommendhist", id=ids, body=json.dumps(hist), doc_type="test")




def hander_lonlat(id, lon, lat, imei, rule, mediaId="yoyo", version=1):

    df_content = get_content()
    df_content = df_content[-((df_content.contentType == "1") & (df_content.priority == 0))]

    try:
        df_content1 = get_content(name='data_content_haikehao')
        df_content1 = df_content1[-(df_content1.priority <= 0)]

        now = datetime.datetime.now()
        day2 = str(now - datetime.timedelta(2))
        df_content1 = df_content1[df_content1.kaTimes > day2]

        df_content.mediaId = df_content.mediaId.map(str)
        df_content = pd.concat([df_content, df_content1])
    except Exception as e:
        pass

    black_list = black_query(mediaId)
    df_content = df_content[- df_content.mediaId.isin(black_list)]
    df_content.loc[df_content.type == 12, 'contentType'] = 6


    df_content = cut_hist(id, imei, df_content, 500)
    res = get_id(lon, lat, df_content, rule)
    try:
        put_es(id, imei, res)  # 记录
    except:
        pass
    res = result_es_out(res, id, imei, version)
    return res




# 只是传入两个参数
def hander_countrycode(id, imei, lon, lat, rule, mediaId="yoyo", version=1):
    df_content = get_content()
    df_content1 = get_content(name='data_content_haikehao')
    df_content1 = df_content1[-(df_content1.priority <= 0)]

    now = datetime.datetime.now()
    day2 = str(now - datetime.timedelta(2))
    df_content1 = df_content1[df_content1.kaTimes > day2]

    df_content = pd.concat([df_content, df_content1])

    df_content.loc[df_content.type == 12, 'contentType'] = 6
    df_content.mediaId = df_content.mediaId.map(str)
    black_list = black_query(mediaId)
    df_content = df_content[- df_content.mediaId.isin(black_list)]
    # df_content1 = get_content(name='data_content_haikehao')
    # df_content = pd.concat([df_content, df_content1])
    changes = True
    df_content = cut_hist(id, imei, df_content, 500)
    res = get_id(lon, lat, df_content, rule, change_weight=changes)
    try:
        put_es(id, imei, res)  
    except:
        pass
    res = result_es_out(res, id, imei, version)
    return res


def compute_lonlat(id, lon, lat, imei, mediaId="yoyo", version=1):

    rule = get_data_streamlet('rule', 1)
    rule = rule[0]


    n_top = int(rule.get('55', 2))
    n_fast = int(rule.get('66', 1))
    remain = int(rule.get('77', 1))

    data_fast = get_data_streamlet('data_fast', n_fast)

    if data_fast != []:

        now = datetime.datetime.now()
        before_30 = str(now - datetime.timedelta(minutes=5))  # 1/48
        time_cond = data_fast[0].get('kaTimes')
        if before_30 > time_cond:
            data_fast = []
    data_fast = pd.DataFrame(data_fast)

    if data_fast.__len__() != 0:
        data_fast.type = data_fast.type.map(str)
        data_fast.loc[data_fast.type == '12', 'contentType'] = 6
        data_fast = data_fast.sort_values('contentType')

    data_fast = result_es_out(data_fast, id, imei, version)

    try:
        if remain == 0:
            raise RuntimeError('testError')

        data_top = get_data_streamlet('data_top', 100)
        data_top = pd.DataFrame(data_top)
        if data_top.__len__() != 0:
            data_top.type = data_top.type.map(str)
            data_top.loc[data_top.type == '12', 'contentType'] = 6
        data_top = data_top.sort_values(['releaseTime'], ascending=[False])
        data_top.type = data_top.type.map(str)
        special = data_top[data_top.type == "12"]
        special = special[:1]
        if special.__len__() == 0: #
            raise RuntimeError('testError')

        now = datetime.datetime.now()
        tmp = special.releaseTime.tolist()
        tmp = datetime.datetime.strptime(tmp[0][:19], "%Y-%m-%d %H:%M:%S")

        diff = int((now-tmp).days)
        if diff > 3:
            raise RuntimeError('testError')

        out1 = result_es_out(special, id, imei, version)
        data_top = data_top[- data_top.contentId.isin(special.contentId.tolist())][:int(n_top -1)]
        if data_top.__len__() != 0:
            data_top = data_top.sort_values('contentType')
            data_top.type = data_top.type.map(str)
            data_top.loc[data_top.type == '12', 'contentType'] = 6

        data_top = data_top.sort_values(['priority', 'releaseTime'], ascending=[False, False])
        data_top = result_es_out(data_top, id, imei, version)
        data_top.extend(out1)

    except:
        data_top = get_data_streamlet('data_top', n_top)
        data_top = pd.DataFrame(data_top)

        if data_top.__len__() != 0:
            data_top = data_top.drop_duplicates(['contentId'])
            data_top = data_top.sort_values('contentType')
            data_top.type = data_top.type.map(str)
            data_top.loc[data_top.type == '12', 'contentType'] = 6

            data_top.priority = data_top.priority.fillna(0)
            data_top.priority = data_top.priority.replace('', 0)
            try:
                data_top.priority = data_top.priority.map(int)
                data_top = data_top.sort_values(['priority', 'releaseTime'], ascending=[False, False])
            except:
                pass

        data_top = result_es_out(data_top, id, imei, version)

    # --数据存储
    put_es_list(id, imei, data_fast)
    put_es_list(id, imei, data_top)

    t = hander_lonlat(id, lon, lat, imei, rule, mediaId, version)

    lens = len(t)

    format_data = []
    format_data.extend(data_fast)
    format_data.extend(data_top)
    format_data.extend(t)

    out = {
        "status": "200",
         "code": "200",
         "msg": "",
         "message": "",
        "recomLen": lens,
         "data": format_data}

    return json.dumps(out)


def compute_contrycode(id, imei, lon, lat,mediaId="yoyo", version=1):
    rule = get_data_streamlet('rule', 1)
    rule = rule[0]


    n_top = int(rule.get('55', 2))
    n_fast = int(rule.get('66', 1))
    remain = int(rule.get('77', 1))

    data_fast = get_data_streamlet('data_fast', n_fast)

    if data_fast != []:
 
        now = datetime.datetime.now()
        before_30 = str(now - datetime.timedelta(minutes=5))  # 1/48
        time_cond = data_fast[0].get('kaTimes')
        if before_30 > time_cond:
            data_fast = []
    data_fast = pd.DataFrame(data_fast)

    if data_fast.__len__() != 0:
        data_fast.type = data_fast.type.map(str)
        data_fast.loc[data_fast.type == '12', 'contentType'] = 6
        data_fast = data_fast.sort_values('contentType')

    data_fast = result_es_out(data_fast, id, imei)

    try:
        if remain == 0:
            raise RuntimeError('testError')

        data_top = get_data_streamlet('data_top', 100)
        data_top = pd.DataFrame(data_top)
        if data_top.__len__() != 0:
            data_top.type = data_top.type.map(str)
            data_top.loc[data_top.type == '12', 'contentType'] = 6
        data_top = data_top.sort_values(['releaseTime'], ascending=[False])
        data_top.type = data_top.type.map(str)
        special = data_top[data_top.type == "12"]
        special = special[:1]
        if special.__len__() == 0: # 跳转
            raise RuntimeError('testError')

        now = datetime.datetime.now()
        tmp = special.releaseTime.tolist()
        tmp = datetime.datetime.strptime(tmp[0][:19], "%Y-%m-%d %H:%M:%S")

        diff = int((now-tmp).days)
        if diff > 3:
            raise RuntimeError('testError')

        out1 = result_es_out(special, id, imei)
        data_top = data_top[- data_top.contentId.isin(special.contentId.tolist())][:int(n_top -1)]
        if data_top.__len__() != 0:
            data_top = data_top.sort_values('contentType')
            data_top.type = data_top.type.map(str)
            data_top.loc[data_top.type == '12', 'contentType'] = 6
        data_top = result_es_out(data_top, id, imei)
        data_top.extend(out1)
    except:
        data_top = get_data_streamlet('data_top', n_top)
        data_top = pd.DataFrame(data_top)

        if data_top.__len__() != 0:
            data_top = data_top.drop_duplicates(['contentId'])
            data_top = data_top.sort_values('contentType')
            data_top.type = data_top.type.map(str)
            data_top.loc[data_top.type == '12', 'contentType'] = 6

            data_top.priority = data_top.priority.fillna(0)
            data_top.priority = data_top.priority.replace('', 0)
            try:
                data_top.priority = data_top.priority.map(int)
                data_top = data_top.sort_values(['priority', 'releaseTime'], ascending=[False, False])
            except:
                pass

        data_top = result_es_out(data_top, id, imei, version)

    # --数据存储
    put_es_list(id, imei, data_fast)
    put_es_list(id, imei, data_top)

    t = hander_countrycode(id, imei, lon, lat, rule, mediaId, version)

    lens = len(t)
    format_data = []
    format_data.extend(data_fast)
    format_data.extend(data_top)
    format_data.extend(t)

    out = {
        "status": "200",
        "code": "200",
        "msg": "",
        "message": "",
        "recomLen": lens,
        "data": format_data}

    out = json.dumps(out)
    return out

# ---海客号---
def hander_lonlat_haikehao(id, lon, lat, imei, rule, mediaId="yoyo", version=1):

    print('aaaa')
    df_content = get_content(name='data_content_haikehao')

    df_content.contentType = df_content.contentType.map(str)
    # --- 只要文章
    df_content = df_content[df_content.contentType.isin(["1"])]
    df_content.mediaId = df_content.mediaId.map(str)
    black_list = black_query(mediaId)
    df_content = df_content[- df_content.mediaId.isin(black_list)]

    df_content = cut_hist(id, imei, df_content)
    print(df_content)
    res = get_id(lon, lat, df_content, rule)
    print('res')
    put_es(id, imei, res)  # 记录

    res = result_es_out(res, id, imei, version)
    return res



# 只是传入两个参数
def hander_countrycode_haikehao(id, country_code, imei, rule, version):
    #country_code = int(country_code)
    df_content = get_content(name='data_content_haikehao')

    df_content = df_content[df_content.contentType.isin(["1"])]

    df_content = cut_hist(id, imei, df_content)  
    res = get_country(country_code, df_content, rule)
    put_es(id, imei, res)
    res = result_es_out(res, id, imei, version)
    return res


def compute_lonlat_haikehao(id, lon, lat, imei, mediaId="yoyo", version=1):
    try:
        rule = get_data_streamlet('rule', 1)
        rule = rule[0]

        # print(rule)
        t = hander_lonlat_haikehao(id, lon, lat, imei, rule, mediaId, version)
        lens = len(t)

        format_data = []
        format_data.extend(t)

        out = {
            "status": "200",
             "code": "200",
             "msg": "",
             "message": "",
            "recomLen": lens,
             "data": format_data}

        return json.dumps(out)
    except Exception as e:
        print(e)


def compute_contrycode_haikehao(id, country_code, imei):

    rule = get_data_streamlet('rule', 1)
    rule = rule[0]
    print('2')
    t = hander_countrycode_haikehao(id, country_code, imei, rule)

    lens = len(t)
    #t = eval(t)
    format_data = []
    format_data.extend(t)

    out = {
        "status": "200",
        "code": "200",
        "msg": "",
        "message": "",
        "recomLen": lens,
        "data": format_data}

    out = json.dumps(out)
    return out


def query_es_hist(xids, current, size, orders="desc"):
    try:
        from elasticsearch import Elasticsearch
        es = Elasticsearch(config.es_ip, port=config.es_port)

        #print(xids)
        #print(current)
        current = int(current)
        size = int(size)

        stamp_time = int(time.time() - 60*60*24*2)
        froms  = current*size - size
        print(froms)
        print("------------a------------------")
        # 没有查询到的
        query = {
            "from": int(froms),
            "size": int(size),
            "query": {
              "bool": {
                  "must": [{
                      "term": {
                          "hisid": {
                              "value": str(xids)
                          }
                      }
                  }]
                  , "must_not": [
                      {"type": {
                          "value": 2
                        }
                      }
                  ]
                  , "filter": {
                      "range": {
                          "recommTimeStamp": {
                              "gte": stamp_time
                          }
                      }
                  }
              }
          },
          "sort": [
                {
                    "recommTimeStamp": {
                        "order": orders
                    }
                }
            ]
        }
        indexs = "recommendhist"
        doc_type1 = "test"

        test = es.search(index=indexs, doc_type=doc_type1, body=query)  # 没有查询到的时候产生[]
        #print(test)
        data_source = test['hits']['hits']
        #print(data_source)
        def get_hist_content(x):
            tmp = x['_source']
            ids = tmp.get("contentId")
            type = tmp.get('type')   # 前面的快讯等
            res = (ids, type)
            return res

        hist_recom = list(map(get_hist_content, data_source))
        ids = [i[0] for i in hist_recom if i[1] != 2]      # type = 2是data_fast, data_top, data_hot
    except Exception as e:
        print(e)
        print('aaaaa')

    return ids




def del_es_his(content_id):
    try:
        from elasticsearch import Elasticsearch
        es = Elasticsearch(config.es_ip, port=config.es_port)

        indexs = "recommendhist"
        doc_type1 = "test"
        content_id = int(content_id)
        query = {
            "query": {
            "term": {
              "contentId": {
                "value": content_id
              }
            }
          }
        }
        res = es.delete_by_query(index=indexs, doc_type= doc_type1, body = query)
        out = {
            'status': 200,
            'del_num': res.get('total')
        }

        return json.dumps(out)
    except Exception as e:
        print(e)
        out = {
            'status': 404,
            'message': 'errors on del_es_his',
            'error': str(e)
        }

        return json.dumps(out)


def ReHist(id, imei, current, size, lon, lat, mediaId="yoyo", version=1):
    import pandas as pd

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
    user_pd = user_pd.drop_duplicates(subset=['contentId'])

    if id:
        xids = str(imei) + "_" + str(id)
    else:
        xids = str(imei)

    rule = get_data_streamlet('rule', 1)
    rule = rule[0]
    if lon == None:lon = 39.39
    if lat == None: lat = 126.33

    temp = hander_lonlat(id, lon, lat, imei, rule)  # 写入历史记录中
    if temp == []:
        try:
            all_data = red.zrangebyscore('data_content', 0, end)

            all_data1 = map(toDict, all_data)
            all_data = list(all_data1)
            user_pd = pd.DataFrame(all_data)

            now = datetime.datetime.now()
            lag_7 = str(now - datetime.timedelta(7))  # 1/48

            user_pd = user_pd[user_pd.releaseTime > lag_7]
            user_pd.mediaId = user_pd.mediaId.map(str)
            black_list = black_query(mediaId)
            user_pd = user_pd[- user_pd.mediaId.isin(black_list)]
    
            res_hist = query_es_hist(xids, 1, 10)

            user_pd = user_pd[-user_pd.contentId.isin(res_hist)]

            user_pd.contentType = user_pd.contentType.map(str)
            user_pd.priority = user_pd.priority.replace('', 0)
            user_pd.priority = user_pd.priority.fillna(int(0))
            user_pd = user_pd[user_pd.contentType.isin(['1', '2', '3', '4', '6'])]
            user_pd = user_pd[-((user_pd.contentType == "3") & (user_pd.priority <= 0))]
            user_pd = user_pd[-((user_pd.contentType == "4") & (user_pd.priority <= 0))]
            user_pd = user_pd[-((user_pd.contentType == "1") & (user_pd.priority == 0))]
            user_pd = user_pd.drop_duplicates(['contentId'])

            user_pd = user_pd.sort_values(['releaseTime'], ascending=[False])

            current = int(current)
            size = int(size)
            froms = current * size - size + current*2
            sizes = froms + size

            print(froms)
            print(sizes)
            df = user_pd[froms:sizes].sample(size)

            out_data = result_es_out(df, id, imei, version)
            out = {
                "status": "200",
                "code": "200",
                "msg": "",
                "message": "",
                "data": out_data}

            out = json.dumps(out)
        except:
            out = {
                "status": "200",
                "code": "200",
                "msg": "",
                "message": "",
                "data": []}

            out = json.dumps(out)
        return out


    user_pd = user_pd.drop_duplicates(['contentId'])

    # 分页进行查询的
    test = query_es_hist(xids, 1, size)

    df = user_pd[user_pd.contentId.isin(test)]

    out_data = result_es_out(df, id, imei, version)
    out = {
        "status": "200",
        "code": "200",
        "msg": "",
        "message": "",
        "data": out_data}

    out = json.dumps(out)

    return out



def query_es_hist_haike(xids, current, size):
    try:
        from elasticsearch import Elasticsearch
        es = Elasticsearch(config.es_ip, port=config.es_port)

        print(xids)
        print(current)
        current = int(current)
        size = int(size)

        froms  = current*size - size
        print(froms)
        print("------------a------------------")
        # 没有查询到的
        query = {
            "from": int(froms),
            "size": int(size),
            "query": {
                "bool": {
                    "must": [
                        {"term": {
                            "hisid": {
                                "value": str(xids)
                                }
                            }
                        }
                        ,{"term": {        
                            "_xid": {
                                "value": "1"
                                }
                            }
                        }
                        ,{"term": {       
                            "plateType": {
                                "value": 3
                                }
                            }
                        }
                    ]
                    , "must_not": [       
                        {"type": {
                            "value": 2
                        }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "recommTimeStamp": {
                        "order": "desc"
                    }
                }
            ]
        }
        indexs = "recommendhist"
        doc_type1 = "test"

        test = es.search(index=indexs, doc_type=doc_type1, body=query)  
        # print(test)
        data_source = test['hits']['hits']
        def get_hist_content(x):
            tmp = x['_source']
            ids = tmp.get("contentId")
            type = tmp.get('type')   
            res = (ids, type)
            return res

        hist_recom = list(map(get_hist_content, data_source))
        ids = [i[0] for i in hist_recom if i[1] != 2]     
    except Exception as e:
        print(e)
        print('aaaaa')

    return ids


def ReHist_haikehao(id, imei, current, size, mediaId="yoyo", lon=39.39, lat=126.33, version=1):
    rule = get_data_streamlet('rule', 1)
    rule = rule[0]
    hander_lonlat_haikehao(id, lon, lat, imei, rule, mediaId, version)
    if id:
        xids = str(imei) + "_" + str(id)
    else:
        xids = str(imei)

    import pandas as pd

    end = int(99999999999999999999999999)
    rdp = redis.ConnectionPool(host=config.redis_ip, port=config.redis_port, db=config.redis_db, password=config.redis_pass)
    red = redis.StrictRedis(connection_pool=rdp)

    all_data = red.zrangebyscore('data_house', 0, end)
    #all_data = [i for i in all_data if 'null' not in i.decode()]


    def toDict(i):
        tmp = i.decode()
        res = json.loads(tmp)
        return res

    all_data1 = map(toDict, all_data)
    all_data = list(all_data1)
    user_pd = pd.DataFrame(all_data)

    user_pd = user_pd.drop_duplicates(['contentId'])
    user_pd.mediaId = user_pd.mediaId.map(str)
    black_list = black_query(mediaId)
    user_pd = user_pd[- user_pd.mediaId.isin(black_list)]
    #print(user_pd)
    #print(xids)

    # 分页进行查询的
    test = query_es_hist_haike(xids, current, size)
    print(test)
    # ---推荐历史
    df = user_pd[user_pd.contentId.isin(test)]
    print(df)


    out_data = result_es_out(df, id, imei, version)
    print('aaaa')
    #out_data = eval(out_data)

    out = {
        "status": "200",
        "code": "200",
        "msg": "",
        "message": "",
        "data": out_data}

    out = json.dumps(out)

    return out