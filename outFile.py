# -*- coding: utf-8 -*-

# @Time :2019/3/7 14:38
# @File: outFile.py
# @目的

from elasticsearch import Elasticsearch
import datetime
import json
import uuid

import ReSysApi2.configs as config
from ReSysApi2.until import supply_up_count, query_last_fresh, query_special_info


config_dict = config.index_stragey

def result_es_out(result, id, imei, version="1", stragey_id = config_dict):
    '''
    :param result: 推荐结果  pandas.DataFrame()
    :param id: 用户名称 str()
    :param imei: 机器码  str()
    :return: json数据
    '''

    scene_id = config_dict.get('scene_id')
    exp_id = config_dict.get('exp_id')
    stragey_id = config_dict.get('stragey_id')
    retrieve_id = config_dict.get('retrieve_id')
    log_id = str(uuid.uuid1())

    # result.contentType = result.contentType.map(str)
    # result.loc[result.type == 12, 'contentType'] = 6
    es = Elasticsearch(config.es_ip, port=config.es_port)
    imei = imei #机器码

    # --记录esid的前缀
    if id:
        id_font = str(imei) + "_" + str(id)
    else:
        id_font = str(imei)

    timeStamp = str(datetime.datetime.now())[:19]


    if result.__len__() != 0:
        dataes = []
        for i in range(result.__len__()):
            datafast = result.iloc[i, ]

            datafast = datafast.to_json()
            datafast = json.loads(datafast)
            try:
                datafast = supply_up_count(datafast, id)
            except Exception as e:
                pass

            ids = datafast.get('contentId')
            ids = id_font + "_" + str(ids)
            # ---contentType ==1 新闻
            try:
                if str(datafast.get("contentType")) not in ['1', '2', '3', '4', '6', '9']:
                    continue

                if str(datafast.get('contentType')) == str(1):
                    last_fresh_data = query_last_fresh(str(datafast.get('contentId')))
                    a = {
                        "id": datafast.get('contentId'),

                        "newsInfo": {
                            "scene_id": scene_id,
                            "exp_id": exp_id,
                            "strategy_id": stragey_id,
                            "retrieve_id": retrieve_id,
                            "log_id": log_id,
                            "distinct_id": str(id),

                            "origin": datafast.get("origin"),
                            "geolocation": datafast.get('geolocation'),
                            "videoUrl": datafast.get('videoUrl'),
                            "videoPlayCount": datafast.get('videoPlayCount'),
                            "videoLength": datafast.get('videoLength'),
                            "viceTitle": datafast.get('viceTitle'),
                            "type": datafast.get('type'),
                            "title": datafast.get('title'),
                            "sourceName": datafast.get('sourceName'),
                            "sourceCode": datafast.get('sourceCode'),
                            "showType": datafast.get('showType'),
                            "shoulderTitle": datafast.get('shoulderTitle'),
                            "releaseTime": datafast.get('releaseTime'),
                            "priority": datafast.get('priority'),
                            "longitude": datafast.get('longitude'),
                            "location": datafast.get('location'),
                            "likeCount": datafast.get('likeCount'),
                            "latitude": datafast.get('latitude'),
                            "keyWord": datafast.get('keyWord'),
                            "imgList": datafast.get('imgList'),
                            "imgClickCount": datafast.get('imgClickCount'),
                            "id": datafast.get('id'),
                            "commentCount": datafast.get('commentCount'),
                            "channel": datafast.get('channel'),
                            "careStatus": datafast.get('careStatus'),
                            "contentId": datafast.get('contentId'),
                            "publishTime": datafast.get('releaseTime'),
                            "outLink": datafast.get('outLink'),
                            "sourceType": datafast.get("sourceType"),
                            "lastFreshNews": last_fresh_data,
                        },
                        "freshnewsInfo": "",
                        "activityContent": "",
                        "specialInfo": "",
                        "eventInfo": "",
                        "contentType": datafast.get('contentType'),
                        "sequence": "",
                        "createTime": timeStamp,
                        "showType": datafast.get('showType')
                    }

                # ---contenType ==2 事件

                if str(datafast.get('contentType')) == str(2):
                    a = {
                        "id": datafast.get('contentId'),

                        "newsInfo": "",
                        "freshnewsInfo": "",
                        "activityContent": "",
                        "specialInfo": "",
                        "eventInfo": {
                            "scene_id": scene_id,
                            "exp_id": exp_id,
                            "strategy_id": stragey_id,
                            "retrieve_id": retrieve_id,
                            "log_id": log_id,
                            "distinct_id": str(id),

                            "videoUrl": datafast.get('videoUrl'),
                            "videoPlayCount": datafast.get('videoPlayCount'),
                            "videoLength": datafast.get('videoLength'),
                            "userName": datafast.get('userName'),
                            "userIcon": datafast.get('userIcon'),
                            "userAccount": datafast.get('userAccount'),
                            "type": datafast.get('type'),
                            "title": datafast.get('title'),
                            "status": datafast.get('status'),
                            "startTime": datafast.get('startTime'),
                            "signupNum": datafast.get('signupNum'),
                            "showType": datafast.get('showType'),
                            "priority": datafast.get('priority'),
                            "longitude": datafast.get('longitude'),
                            "location": datafast.get('location'),
                            "likeCount": datafast.get('likeCount'),
                            "latitude": datafast.get('latitude'),
                            "keyWord": datafast.get('keyWord'),
                            "imgList": datafast.get('imgList'),
                            "imgClickCount": datafast.get('imgClickCount'),
                            "id": datafast.get('id'),
                            "endTime": datafast.get('endTime'),
                            "distance": datafast.get('distance'),
                            "commentCount": datafast.get('commentCount'),
                            "careStatus": datafast.get('careStatus'),
                            "activityCost": datafast.get('activityCost'),
                            "contentId": datafast.get('contentId'),
                            "publishTime": datafast.get('releaseTime'),
                        },
                        "contentType": datafast.get('contentType'),
                        "sequence": "",
                        "createTime": timeStamp,
                        "showType": datafast.get('showType')
                    }

                # ---contenType ==3 新鲜事
                # if datafast.contentType.item() == 3
                if str(datafast.get('contentType')) == str(3):
                    if datafast.get("user_is_up") == True:
                        datafast['up'] = "y"
                    else:
                        datafast['up'] = "n"

                    if datafast.get("comment_count") == "":
                        datafast['comment_count'] = 0

                    a = {
                        "id": datafast.get('contentId'),

                        "newsInfo": "",
                        "freshnewsInfo": {
                            "scene_id": scene_id,
                            "exp_id": exp_id,
                            "strategy_id": stragey_id,
                            "retrieve_id": retrieve_id,
                            "log_id": log_id,
                            "distinct_id": str(id),

                            "anonymous": datafast.get("anonymous"),
                            "auditMsg": datafast.get("auditMsg"),
                            "auditStatus": datafast.get("auditStatus"),
                            "auditor": datafast.get("auditor"),
                            "commentNum": datafast.get("comment_count"),
                            "concernType": datafast.get("concernType"),
                            "content": datafast.get("content"),
                            "countryCode": datafast.get("countryCode"),
                            "createTime": datafast.get("createTime"),
                            "describes": datafast.get("describes"),
                            "detailedAddress": datafast.get("detailedAddress"),
                            "distance": datafast.get("distance"),
                            "fileId": datafast.get("fileId"),
                            "fileList": datafast.get("fileList"),
                            "freshnewsId": datafast.get("freshnewsId"),
                            "freshnewsType": datafast.get("freshnewsType"),
                            "geolocation": datafast.get("geolocation"),
                            "index": 2,
                            "icon": datafast.get("icon"),
                            "imeiNo": datafast.get("imeiNo"),
                            "isAdminSet": datafast.get("isAdminSet"),
                            "isPublic": datafast.get("isPublic"),
                            "lastModifyTime": datafast.get("lastModifyTime"),
                            "latitude": datafast.get("latitude"),
                            "longitude": datafast.get("longitude"),
                            "mediaId": datafast.get("mediaId"),
                            "name": datafast.get("name"),
                            "priority": datafast.get("priority"),
                            "relatednewsId": datafast.get("relatednewsId"),
                            "relatednewsTitle": datafast.get("relatednewsTitle"),
                            "relatednewsType": datafast.get("relatednewsType"),
                            "relatednewsUrl": datafast.get("relatednewsUrl"),
                            "releaseTime": datafast.get("releaseTime"),
                            "sex": datafast.get("sex"),
                            "siteId": datafast.get("siteId"),
                            "sourceType": datafast.get("sourceType"),
                            "status": datafast.get("status"),
                            "topicId": datafast.get("topicId"),
                            "topicName": datafast.get("topicName"),
                            "up": datafast.get("up"),
                            "upNum": datafast.get("up_count"),
                            "url": datafast.get("url"),
                            "userId": datafast.get("userId"),
                            "videoCount": datafast.get("videoCount"),
                            "videoImgUrl": datafast.get("videoImgUrl"),
                            "videoMsg": datafast.get("videoMsg"),
                            "videoTime": datafast.get("videoTime"),
                            "videoUrl": datafast.get("videoUrl"),
                            "contentId": datafast.get('contentId'),
                            "publishTime": datafast.get('releaseTime')
                        },
                        "activityContent": "",
                        "specialInfo": "",
                        "eventInfo": "",
                        "contentType": datafast.get('contentType'),
                        "sequence": "",
                        "createTime": timeStamp,
                        "showType": datafast.get('showType')
                    }

                # ---活动---
                if str(datafast.get('contentType')) == str(4):
                    a = {
                        "id": datafast.get('contentId'),

                        "newsInfo": "",
                        "freshnewsInfo": "",
                        "activityContent": {
                            "scene_id": scene_id,
                            "exp_id": exp_id,
                            "strategy_id": stragey_id,
                            "retrieve_id": retrieve_id,
                            "log_id": log_id,
                            "distinct_id": str(id),

                            "activityId": datafast.get("activityId"),
                            "title": datafast.get("title"),
                            "parentTypeId": datafast.get("parentTypeId"),
                            "typeId": datafast.get("typeId"),
                            "activityType": datafast.get("activityType"),
                            "thumb": datafast.get("thumb"),
                            "content": datafast.get("content"),
                            "starttime": datafast.get("starttime"),
                            "endtime": datafast.get("endtime"),
                            "country": datafast.get("country"),
                            "province": datafast.get("province"),
                            "city": datafast.get("city"),
                            "detailedAddress": datafast.get("detailedAddress"),
                            "signupNum":datafast.get("signupNum"),
                            "registerNum": datafast.get("registerNum"),
                            "activityCost": datafast.get("activityCost"),
                            "priority": datafast.get("priority"),
                            "sourceType": datafast.get("sourceType"),
                            "mediaId": datafast.get("mediaId"),
                            "userId": datafast.get("userId"),
                            "name": datafast.get("name"),
                            "sex": datafast.get("sex"),
                            "icon": datafast.get("icon"),
                            "describes": datafast.get("describes"),
                            "countryCode": datafast.get("countryCode"),
                            "longitude": datafast.get("longitude"),
                            "latitude": datafast.get("latitude"),
                            "necessaryInfo": datafast.get("necessaryInfo"),
                            "releaseTime": datafast.get("releaseTime"),
                            "shareTitle": datafast.get("shareTitle"),
                            "shareUrl": datafast.get("shareUrl"),
                            "twoDimensionCodeUrl": datafast.get("twoDimensionCodeUrl"),
                            "auditor": datafast.get("auditor"),
                            "auditMsg": datafast.get("auditMsg"),
                            "auditStatus": datafast.get("auditStatus"),
                            "status": datafast.get("status"),
                            "createTime": datafast.get("createTime"),
                            "lastModifyTime": datafast.get("lastModifyTime"),
                            "signupRate": datafast.get("signupRate"),
                            "distance": datafast.get("distance"),
                            "isRegister": datafast.get("isRegister"),
                            "askGroupId": datafast.get("askGroupId"),
                            "commentGroupId": datafast.get("commentGroupId")
                        },
                        "specialInfo": "",
                        "eventInfo": "",
                        "contentType": datafast.get('contentType'),
                        "sequence": "",
                        "createTime": timeStamp,
                        "showType": datafast.get('showType')
                    }

                # ---contenType == 6 专题
                # if datafast.contentType.item() == 6
                if str(datafast.get('contentType')) == str(6):
                    # ---datafast1 是专题的
                    # ---datafast 是文章的
                    print(datafast)
                    if str(version) == "1":
                        a = {
                            "id": datafast.get('contentId'),

                            "newsInfo": "",
                            "freshnewsInfo": "",
                            "activityContent": "",
                            "specialInfo": {
                                "scene_id": scene_id,
                                "exp_id": exp_id,
                                "strategy_id": stragey_id,
                                "retrieve_id": retrieve_id,
                                "log_id": log_id,
                                "distinct_id": str(id),

                                "videoUrl": datafast.get('videoUrl'),
                                "videoPlayCount": datafast.get('videoPlayCount'),
                                "videoLength": datafast.get('videoLength'),
                                "viceTitle": datafast.get('viceTitle'),
                                "type": datafast.get('type'),
                                "title": datafast.get('title'),
                                "specialName": datafast.get('specialName'),
                                "specialId": datafast.get('specialId'),
                                "sourceName": datafast.get('sourceName'),
                                "sourceCode": datafast.get('sourceCode'),
                                "showType": datafast.get('showType'),
                                "releaseTime": datafast.get('releaseTime'),
                                "priority": datafast.get('priority'),
                                "longitude": datafast.get('longitude'),
                                "location": datafast.get('location'),
                                "likeCount": datafast.get('likeCount'),
                                "latitude": datafast.get('latitude'),
                                "keyWord": datafast.get('keyWord'),
                                "imgList": datafast.get('imgList'),
                                "imgClickCount": datafast.get('imgClickCount'),
                                "id": datafast.get('id'),
                                "commentCount": datafast.get('commentCount'),
                                "careStatus": datafast.get('careStatus'),
                                "contentId": datafast.get('contentId'),
                                "sourceType": datafast.get("sourceType"),
                                "publishTime": datafast.get('releaseTime')
                            },
                            "eventInfo": "",
                            "contentType": datafast.get('contentType'),
                            "sequence": "",
                            "createTime": timeStamp,
                            "showType": datafast.get('showType')
                        }
                    else:
                        datafast1 = datafast
                        datafast = query_special_info(datafast.get('contentId'))
                        if datafast != "":
                            a = {
                                "id": datafast1.get('contentId'),

                                "newsInfo": "",
                                "freshnewsInfo": "",
                                "activityContent": "",
                                "specialInfo": {

                                    "scene_id": scene_id,
                                    "exp_id": exp_id,
                                    "strategy_id": stragey_id,
                                    "retrieve_id": retrieve_id,
                                    "log_id": log_id,
                                    "distinct_id": str(id),

                                    "origin": datafast.get("origin"),
                                    "geolocation": datafast.get('geolocation'),
                                    "videoUrl": datafast.get('videoUrl'),
                                    "videoPlayCount": datafast.get('videoPlayCount'),
                                    "videoLength": datafast.get('videoLength'),
                                    "viceTitle": datafast.get('viceTitle'),
                                    "type": datafast.get('type'),
                                    "title": datafast.get('title'), # datafast1.get('title')
                                    "specialName": datafast1.get('title'), # datafast1.get('specialName')
                                    "specialId": datafast1.get('id'),
                                    "sourceName": datafast.get('sourceName'),
                                    "sourceCode": datafast.get('sourceCode'),
                                    "showType": datafast1.get('showType'),
                                    "releaseTime": datafast1.get('releaseTime'),
                                    "priority": datafast1.get('priority'),
                                    "longitude": datafast.get('longitude'),
                                    "location": datafast.get('location'),
                                    "likeCount": datafast.get('likeCount'),
                                    "latitude": datafast.get('latitude'),
                                    "keyWord": datafast.get('keyWord'),
                                    # "imgList": datafast1.get('imgList'), # 这个是专题
                                    "imgList": datafast.get('imgList'),  # 这个稿件的
                                    "imgClickCount": datafast.get('imgClickCount'),
                                    "id": datafast1.get('id'),
                                    "commentCount": datafast.get('commentCount'),

                                    "careStatus": datafast1.get('careStatus'),
                                    "contentId": datafast.get('contentId'),
                                    "sourceType": datafast1.get("sourceType"),  # 这个是专题的
                                    "publishTime": datafast1.get('releaseTime'),
                                    "articleId": datafast.get("id"),
                                    "articelTitle": datafast1.get("title")
                                },
                                "eventInfo": "",
                                "contentType": datafast1.get('contentType'),
                                "sequence": "",
                                "createTime": timeStamp,
                                "showType": datafast1.get('showType')
                            }
                        else:
                            a = {
                                "id": datafast.get('contentId'),

                                "newsInfo": "",
                                "freshnewsInfo": "",
                                "activityContent": "",
                                "specialInfo": {
                                    "scene_id": scene_id,
                                    "exp_id": exp_id,
                                    "strategy_id": stragey_id,
                                    "retrieve_id": retrieve_id,
                                    "log_id": log_id,
                                    "distinct_id": str(id),

                                    "videoUrl": datafast.get('videoUrl'),
                                    "videoPlayCount": datafast.get('videoPlayCount'),
                                    "videoLength": datafast.get('videoLength'),
                                    "viceTitle": datafast.get('viceTitle'),
                                    "type": datafast.get('type'),
                                    "title": datafast.get('title'),
                                    "specialName": datafast.get('specialName'),
                                    "specialId": datafast.get('specialId'),
                                    "sourceName": datafast.get('sourceName'),
                                    "sourceCode": datafast.get('sourceCode'),
                                    "showType": datafast.get('showType'),
                                    "releaseTime": datafast.get('releaseTime'),
                                    "priority": datafast.get('priority'),
                                    "longitude": datafast.get('longitude'),
                                    "location": datafast.get('location'),
                                    "likeCount": datafast.get('likeCount'),
                                    "latitude": datafast.get('latitude'),
                                    "keyWord": datafast.get('keyWord'),
                                    "imgList": datafast.get('imgList'),
                                    "imgClickCount": datafast.get('imgClickCount'),
                                    "id": datafast.get('id'),
                                    "commentCount": datafast.get('commentCount'),
                                    "careStatus": datafast.get('careStatus'),
                                    "contentId": datafast.get('contentId'),
                                    "publishTime": datafast.get('releaseTime')
                                },
                                "eventInfo": "",
                                "contentType": datafast.get('contentType'),
                                "sequence": "",
                                "createTime": timeStamp,
                                "showType": datafast.get('showType')
                            }

                # ---contentType ==9 小视频的
                if str(datafast.get('contentType')) == str(9):
                    a = {
                        "id": datafast.get('contentId'),

                        "newsInfo": "",
                        "freshnewsInfo": {
                            "scene_id": scene_id,
                            "exp_id": exp_id,
                            "strategy_id": stragey_id,
                            "retrieve_id": retrieve_id,
                            "log_id": log_id,
                            "distinct_id": str(id),

                            "comment_count": datafast.get("comment_count"),
                            "releaseTime": datafast.get("releaseTime"),
                            "distance": datafast.get("distance"),
                            "columns": datafast.get("columns"),
                            "latitude": datafast.get("latitude"),
                            "channel": datafast.get("channel"),
                            "mid": datafast.get("mid"),
                            "media": datafast.get("media"),
                            "down_count": datafast.get("down_count"),
                            "video": datafast.get("video"),
                            "title": datafast.get("title"),
                            "up_count": datafast.get("up_count"),
                            "mediaId": datafast.get("mediaId"),
                            "source_url": datafast.get("source_url"),
                            "user_is_collect": datafast.get("user_is_collect"),
                            "beautify_multiple": datafast.get("beautify_multiple"),
                            "content_type": datafast.get("content_type"),
                            "unqId": datafast.get("unqId"),
                            "user_is_up": datafast.get("user_is_up"),
                            "id": datafast.get("id"),
                            "source_name": datafast.get("source_name"),
                            "longitude": datafast.get("longitude"),
                            "summary": datafast.get("summary"),
                            "ext": datafast.get("ext"),
                            "imgs": datafast.get("imgs"),
                            "creator": datafast.get("creator"),
                            "favor_count": datafast.get("favor_count"),
                            "index": 4,
                            "video_length": datafast.get("video_length"),
                            "source_type": datafast.get("source_type"),
                            "source_icon": datafast.get("source_icon"),
                            "createTime": datafast.get("createTime"),
                            "comment_type": datafast.get("comment_type"),
                            "topic": datafast.get("topic"),
                            "auditStatus": datafast.get("auditStatus"),
                            "source_id": datafast.get("source_id"),
                            "view_count": datafast.get("view_count"),
                            "status": datafast.get("status"),
                            "geolocation": datafast.get("geolocation"),

                        },
                        "activityContent": "",
                        "specialInfo": "",
                        "eventInfo": "",
                        "contentType": datafast.get('contentType'),
                        "sequence": "",
                        "createTime": timeStamp,
                        "showType": datafast.get('showType')
                    }
                #print(datafast.get("imgList"))
                dataes.append(a)
            except:
                pass
        out_data = dataes
    else:
        out_data = []

    return out_data
