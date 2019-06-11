# -*- coding: utf-8 -*-

# @Time :2018/12/7 18:28
# @File: MessageHandler.py
# @目的



import requests
import json
import redis
import ReSysApi2.configs as config
from pykafka import KafkaClient
from ReSysApi2.RedisDataHandOut import data_hand_out
import datetime

rdp = redis.ConnectionPool(host=config.redis_ip, port=config.redis_port, db=config.redis_db, password=config.redis_pass)
red = redis.StrictRedis(connection_pool=rdp)





def transform_request_data(data_from_serive, contentId, mesReType, kaTimes):


    anonymous = data_from_serive.get("anonymous", 0)
    auditStatus = data_from_serive.get("auditStatus", 0)
    content = data_from_serive.get("content", "")
    countryCode = data_from_serive.get("countryCode", "")
    createTime = data_from_serive.get("createTime", "2018-12-04 15:51:23")
    detailedAddress = data_from_serive.get("detailedAddress", "")
    editorId = data_from_serive.get("editorId", 0)
    freshnewsId = data_from_serive.get("freshnewsId", 0)
    freshnewsType = data_from_serive.get("freshnewsType", 0)
    geolocation = data_from_serive.get("geolocation", "")           # ---并不知道
    isPublic = data_from_serive.get("isPublic", 0)
    lastModifyTime = data_from_serive.get("lastModifyTime", "2018-12-04 15:51:23")
    latitude = data_from_serive.get("latitude", 0)
    longitude = data_from_serive.get("longitude", 0)
    mediaId = data_from_serive.get("mediaId", 0)
    priority = data_from_serive.get("priority", 0)
    relatednewsId = data_from_serive.get("relatednewsId", 0)
    sourceType = data_from_serive.get("sourceType", 0)
    status = data_from_serive.get("status", 0)
    topicId = data_from_serive.get("topicId", 0)
    topicName = data_from_serive.get("topicName", "")
    videoCount = data_from_serive.get("videoCount", 0)
    videoImgUrl = data_from_serive.get("videoImgUrl", "")
    videoMsg = data_from_serive.get("videoMsg", "")
    videoTime = data_from_serive.get("videoTime", 0)
    videoUrl = data_from_serive.get("videoUrl", "")
    activityCost = data_from_serive.get("activityCost", 0)
    activityId = data_from_serive.get("activityId", 0)
    activityType = data_from_serive.get("activityType", "")
    askGroupId = data_from_serive.get("askGroupId", "")
    auditMsg = data_from_serive.get("auditMsg", "")
    auditor = data_from_serive.get("auditor", 0)
    city = data_from_serive.get("city", "")
    commentGroupId = data_from_serive.get("commentGroupId", "")
    country = data_from_serive.get("country", "")
    describes = data_from_serive.get("describes", "")
    distance = data_from_serive.get("distance", 0)
    endtime = data_from_serive.get("endtime", "2018-12-04 15:51:23")
    icon = data_from_serive.get("icon", "")
    isRegister = data_from_serive.get("isRegister", 0)  # 没有赋值
    name = data_from_serive.get("name", "")
    necessaryInfo = data_from_serive.get("necessaryInfo", "")
    parentTypeId = data_from_serive.get("parentTypeId", 0)
    province = data_from_serive.get("province", "")
    registerNum = data_from_serive.get("registerNum", 0)
    releaseTime = data_from_serive.get("releaseTime", "2018-12-04 15:51:23")
    sex = data_from_serive.get("sex", "")
    shareTitle = data_from_serive.get("shareTitle", "")
    shareUrl = data_from_serive.get("shareUrl", "")
    signupNum = data_from_serive.get("signupNum", 0)
    signupRate = data_from_serive.get("signupRate", "")
    starttime = data_from_serive.get("starttime", "2018-12-04 15:51:23")
    thumb = data_from_serive.get("thumb", "")
    title = data_from_serive.get("title", "")
    twoDimensionCodeUrl = data_from_serive.get("twoDimensionCodeUrl", "")
    typeId = data_from_serive.get("typeId", 0)
    userId = data_from_serive.get("userId", 0)
    careStatus = data_from_serive.get("careStatus", "")
    channel = data_from_serive.get("channel", "")
    channelId = data_from_serive.get("channelId", "")
    commentCount = data_from_serive.get("commentCount", "")
    commentType = data_from_serive.get("commentType", "")
    creatorId = data_from_serive.get("creatorId", "")
    description = data_from_serive.get("description", '')
    id = data_from_serive.get("id", 0)
    imgClickCount = data_from_serive.get("imgClickCount", 0)
    imgList = data_from_serive.get("imgList", [])
    keyWord = data_from_serive.get("keyWord", "")
    likeCount = data_from_serive.get("likeCount", 0)
    location = data_from_serive.get("location", "")
    mediaIcon = data_from_serive.get("mediaIcon", "")
    shoulderTitle = data_from_serive.get("shoulderTitle", '')
    showType = data_from_serive.get("showType", '')
    sourceCode = data_from_serive.get("sourceCode", '')
    sourceName = data_from_serive.get("sourceName", '')
    summary = data_from_serive.get("summary", '')
    tags = data_from_serive.get("tags", '')
    type = data_from_serive.get("type", 0)
    upCount = data_from_serive.get("upCount", 0)
    viceTitle = data_from_serive.get("viceTitle", '')
    videoLength = data_from_serive.get("videoLength", 0)
    videoPlayCount = data_from_serive.get("videoPlayCount", 0)
    videoTaskId = data_from_serive.get("videoTaskId", 0)

    commentNum = data_from_serive.get("commentNum", 0)
    concernType = data_from_serive.get("concernType", 0)
    fileId = data_from_serive.get("fileId", "")
    fileList = data_from_serive.get("fileList", "")
    imeiNo = data_from_serive.get("imeiNo", "")
    isAdminSet = data_from_serive.get("isAdminSet", 0)
    relatednewsTitle = data_from_serive.get("relatednewsTitle", "")
    relatednewsType = data_from_serive.get("relatednewsType", 0)
    siteId = data_from_serive.get("siteId", 0)
    relatednewsUrl = data_from_serive.get("relatednewsUrl", "")
    up = data_from_serive.get("up", 0)
    upNum = data_from_serive.get("upNum", 0)
    url = data_from_serive.get("url", "")


    specialName = data_from_serive.get("specialName", "")
    specialId = data_from_serive.get("specialId", "")

    contentType = data_from_serive.get('contentType', 99)
    if contentType==99:
        # contentType = data_from_serive.get('content_type', 99)
        contentType = 9

    userName = data_from_serive.get('userName', "")
    userIcon = data_from_serive.get('userIcon', "")
    userAccount = data_from_serive.get("userAccount", "")

    startTime = data_from_serive.get("startTime", "2018-12-04 15:51:23")
    endTime = data_from_serive.get("endTime", "2018-12-04 15:51:23")
    trackPathUrl = data_from_serive.get("trackPathUrl", "")
    startLocation = data_from_serive.get("startLocation", "")
    endLocation = data_from_serive.get("endLocation", "")

    origin = data_from_serive.get("origin", "")
    atlas = data_from_serive.get("atlas", "")
    upFlag = data_from_serive.get("upFlag", "")
    mediaName = data_from_serive.get("mediaName", "")

    columns = data_from_serive.get("columns", "[]")
    mid = data_from_serive.get("mid", "")
    media = data_from_serive.get("media", "")
    down_count = data_from_serive.get("down_count", 0)
    video = data_from_serive.get("video", "")
    up_count = data_from_serive.get("up_count", 0)
    source_url = data_from_serive.get("source_url", "")
    content_type = data_from_serive.get("content_type","")  #
    beautify_multiple = data_from_serive.get("beautify_multiple", 1)
    user_is_collect = data_from_serive.get("user_is_collect", "")
    unqId = data_from_serive.get("unqId", "")
    user_is_up = data_from_serive.get("user_is_up", "")
    source_name = data_from_serive.get("source_name", "")
    ext = data_from_serive.get("ext", "")
    imgs = data_from_serive.get("imgs", "")
    creator = data_from_serive.get("creator", "")
    favor_count = data_from_serive.get("favor_count", "")
    video_length = data_from_serive.get("video_length", "")
    source_type = data_from_serive.get("source_type", "")
    tags = data_from_serive.get("tags", "")
    share_count = data_from_serive.get("share_count", "")
    source_icon = data_from_serive.get("source_icon", "")
    createTime = data_from_serive.get("createTime", "")
    comment_type = data_from_serive.get("comment_type", "")
    topic = data_from_serive.get("topic", "")
    auditStatus = data_from_serive.get("auditStatus", "")
    source_id = data_from_serive.get("source_id", "")
    view_count = data_from_serive.get("view_count", "")
    status = data_from_serive.get("status", "")
    index = data_from_serive.get("index", "")

    comment_count = data_from_serive.get("comment_count", "")
    outLink = data_from_serive.get('outLink', '')
    redis_data = {
        "anonymous": anonymous,
        "auditStatus": auditStatus,
        "content": content,
        "countryCode": countryCode,
        "createTime": createTime,
        "detailedAddress": detailedAddress,
        "editorId": editorId,
        "freshnewsId": freshnewsId,
        "freshnewsType": freshnewsType,
        "geolocation": geolocation,
        "isPublic": isPublic,
        "lastModifyTime": lastModifyTime,
        "latitude": latitude,
        "longitude": longitude,
        "mediaId": mediaId,
        "priority": priority,
        "relatednewsId": relatednewsId,
        "sourceType": sourceType,
        "status": status,
        "topicId": topicId,
        "topicName": topicName,
        "videoCount": videoCount,
        "videoImgUrl": videoImgUrl,
        "videoMsg": videoMsg,
        "videoTime": videoTime,
        "videoUrl": videoUrl,
        "activityCost": activityCost,
        "activityId": activityId,
        "activityType": activityType,
        "askGroupId": askGroupId,
        "auditMsg": auditMsg,
        "auditor": auditor,
        "city": city,
        "commentGroupId": commentGroupId,
        "country": country,
        "describes": describes,
        "distance": distance,
        "endtime": endtime,
        "icon": icon,
        "isRegister": isRegister,
        "name": name,
        "necessaryInfo": necessaryInfo,
        "parentTypeId": parentTypeId,
        "province": province,
        "registerNum": registerNum,
        "releaseTime": releaseTime,
        "sex": sex,
        "shareTitle": shareTitle,
        "shareUrl": shareUrl,
        "signupNum": signupNum,
        "signupRate": signupRate,
        "starttime": starttime,
        "thumb": thumb,
        "title": title,
        "twoDimensionCodeUrl": twoDimensionCodeUrl,
        "typeId": typeId,
        "userId": userId,
        "careStatus": careStatus,
        "channel": channel,
        "channelId": channelId,
        "commentCount": commentCount,
        "commentType": commentType,
        "creatorId": creatorId,
        "description": description,
        "id": id,
        "imgClickCount": imgClickCount,
        "imgList": imgList,
        "keyWord": keyWord,
        "likeCount": likeCount,
        "location": location,
        "mediaIcon": mediaIcon,
        "shoulderTitle": shoulderTitle,
        "showType": showType,
        "sourceCode": sourceCode,
        "sourceName": sourceName,
        "summary": summary,
        "tags": tags,
        "type": type,
        "upCount": upCount,
        "viceTitle": viceTitle,
        "videoLength": videoLength,
        "videoPlayCount": videoPlayCount,
        "videoTaskId": videoTaskId,

        "type_from_message": mesReType,
        "contentId": contentId,
        "contentType": contentType,
        "startTime": startTime,
        "endTime": endTime,
        "trackPathUrl": trackPathUrl,
        "startLocation": startLocation,
        "endLocation": endLocation,
        "userName": userName,
        "userIcon": userIcon,
        "userAccount": userAccount,

        "commentNum": commentNum,
        "concernType": concernType,
        "fileId": fileId,
        "fileList": fileList,
        "imeiNo": imeiNo,
        "isAdminSet": isAdminSet,
        "relatednewsTitle": relatednewsTitle,
        "relatednewsType": relatednewsType,
        "siteId": siteId,
        "relatednewsUrl": relatednewsUrl,
        "up": up,
        "upNum": upNum,
        "url": url,
        "specialName": specialName,
        "specialId": specialId,

        "columns": columns,
        "comment_count": comment_count,
        "mid": mid,
        "media": media,
        "down_count": down_count,
        "video": video,
        "up_count": up_count,
        "source_url": source_url,
        "user_is_collect": user_is_collect,
        "beautify_multiple": beautify_multiple,
        "content_type": content_type,
        "unqId": unqId,
        "user_is_up": user_is_up,
        "source_name": source_name,
        "ext": ext,
        "imgs": imgs,
        "creator": creator,
        "favor_count": favor_count,
        "index": index,
        "video_length": video_length,
        "source_type": source_type,

        "share_count": share_count,
        "source_icon": source_icon,

        "topic": topic,

        "source_id": source_id,
        "view_count": view_count,

        "outLink": outLink,
        "origin": origin,
        "atlas": atlas,
        "upFlag": upFlag,
        "mediaName": mediaName,

        "kaTimes": kaTimes
    }
    return redis_data



def hander(test):
    '''
    test 中是kafka种的相关消息进行的
    :param test:
    :return:
    '''
    try:
        indexs =test.index(b'{')
        kafka_message = test[indexs:]
        kafka_message = json.loads(kafka_message.decode())

        method = kafka_message.get("method")
        service = kafka_message.get("service")
        service = service.replace('hk-service-platform-gateway', config.gateway_ip)
        contentId = kafka_message.get("contentId")
        #contentId = int(contentId)

        # title -> title
        # thumbnail -> imgList
        # summary -> summary
        title = kafka_message.get('title')
        thumbnail = kafka_message.get('thumbnail')
        summary = kafka_message.get('summary')

        thumbnail = {
            "imgUrl": thumbnail,
            "sortNum": 0,
            "describe": "",
            "imgType": 1
        }

        type_from_message = kafka_message.get("from")
        operate = kafka_message.get("operate")

        data1 = {
            "indexName": "index_news",
            "id": contentId
        }

        kaTimes = str(datetime.datetime.now())


        url = service + '/' + type_from_message + '/' + contentId

        if method == 'post':
            req = requests.post(service, json=data1)

        if method == 'get':
            req = requests.get(url)

        if method == 'put':
            req = requests.put(url)

        if method == 'delete':
            req = requests.delete(url)

        result = json.loads(req.content.decode())

        if type_from_message == "activity":
            mesReType = "1"
        elif type_from_message == "video":
            mesReType = "2"
        elif type_from_message == "article":
            mesReType = "3"
        elif type_from_message == "audio":
            mesReType = "4"
        elif type_from_message == "link":
            mesReType = "5"
        elif type_from_message == "pictures":
            mesReType = "6"
        elif type_from_message == "newthings":
            mesReType = "7"
        else:
            mesReType = "0"

        # 删除下
        if operate =="002":
            red.zremrangebyscore('data_house', contentId, contentId)
            red.zremrangebyscore('data_top', contentId, contentId)
            red.zremrangebyscore('data_fast', contentId, contentId)
            red.zremrangebyscore('data_content', contentId, contentId)
            red.zremrangebyscore('data_content_haikehao', contentId, contentId)
            return

        data_from_serive = result.get("data")
        contentId = int(contentId)

        if title:
            data_from_serive['title'] = title
            # if thumbnail != "":
            #     data_from_serive['imgList'] = [thumbnail]
            data_from_serive['summary'] = summary

        data_house = transform_request_data(data_from_serive, contentId, mesReType, kaTimes)

        #print(data_house)
        if operate =="001":
            red.zremrangebyscore('data_house', contentId, contentId)
            red.zremrangebyscore('data_top', contentId, contentId)
            red.zremrangebyscore('data_fast', contentId, contentId)
            red.zremrangebyscore('data_content', contentId, contentId)
            red.zremrangebyscore('data_content_haikehao', contentId, contentId)

            red.zadd('data_house', contentId, json.dumps(data_house))
            data_hand_out(data_house)

        if operate =="003":
            red.zremrangebyscore('data_house', contentId, contentId)
            red.zremrangebyscore('data_top', contentId, contentId)
            red.zremrangebyscore('data_fast', contentId, contentId)
            red.zremrangebyscore('data_content', contentId, contentId)
            red.zremrangebyscore('data_content_haikehao', contentId, contentId)

            red.zadd('data_house', contentId, json.dumps(data_house))
            print('begin')
            data_hand_out(data_house)

    except Exception as e:
        print('----')
        print(e)


if __name__ == '__main__':
    client = KafkaClient(hosts=config.kafka_config, zookeeper_hosts=config.zookeep_config)

    topic = client.topics[b'haike-recommend']
    consumer = topic.get_simple_consumer()

    for message in consumer:
        if message is not None:
            # print(message.offset, message.value)
            print(message.offset)
            value = message.value
            hander(value)



