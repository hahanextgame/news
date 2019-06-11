# -*- coding: utf-8 -*-

# @Time :2019/2/19 16:37
# @File: trans.py
# @目的

import requests
import json
import datetime
from ReSysApi2.until import supply_up_count



def transfer(content_name, content_id):
    content_name = str(content_name)
    content_id = str(content_id)

    base_url = ''
    url = base_url + '/' + content_name + '/' + content_id

    req = requests.get(url)
    result = json.loads(req.content.decode())

    data_from_serive = result.get("data")

    all_data = transform_request_data(data_from_serive)

    out_data = clean_esdata(all_data)

    json.dumps(out_data)


def transform_request_data(data_from_serive):

    contentId = data_from_serive.get('id')
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
    comment_count = data_from_serive.get("comment_count", "")
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

    ## 
    if contentType==99:
#        contentType = data_from_serive.get('content_type', 99)
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

        "origin": origin,
        "atlas": atlas,
        "upFlag": upFlag,
        "mediaName": mediaName,

        "columns": columns,
        "comment_count": comment_count,
        "mid": mid,
        "media":media,
        "down_count": down_count,
        "video":video,
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
        "outLink": outLink

    }
    return redis_data



def clean_esdata(datafast):
    timeStamp = str(datetime.datetime.now())[:19]

    if datafast.get('type') == 12:
        datafast['contentType'] = 6

    if str(datafast.get('contentType')) == '':
        return

    if str(datafast.get('contentType')) == str(1):
        if str(datafast.get('type')) == str(2) or str(datafast.get('type')) == str(3):
            a = {
                "id": datafast.get('contentId'),
                "newsInfo": {
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
                    "outLink":datafast.get("outLink")
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
        else:
            a = {
            "id": datafast.get('contentId'),
            "newsInfo": {
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
                "publishTime": datafast.get('releaseTime')
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
    # if datafast.contentType.item() == 2
    if str(datafast.get('contentType')) == str(2):
        a = {
            "id": datafast.get('contentId'),
            "newsInfo": "",
            "freshnewsInfo": "",
            "activityContent": "",
            "specialInfo": "",
            "eventInfo": {
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
                "signupNum": datafast.get("signupNum"),
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


    if str(datafast.get('contentType')) == str(6):
        a = {
            "id": datafast.get('contentId'),
            "newsInfo": "",
            "freshnewsInfo": "",
            "activityContent": "",
            "specialInfo": {
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


    if str(datafast.get('contentType')) == str(9):
        a = {
            "id": datafast.get('contentId'),
            "newsInfo": "",
            "freshnewsInfo": {
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
    return a


def query_subject(data):

    url = ''

    res = requests.post(url, json=data)
    res = json.loads(res.content).get('data')
    return res



def query_subject_es(two):
    res = two[0]
    user_id = two[1]
    cond1 = res.get('nodeType')
    content_id = res.get('newsCode')

    if cond1 == 9:
        content_name = 'video'
    elif cond1 == 8:
        content_name = 'newthings'
    else:
        content_name = 'article'

    base_url = ''
    
    url = base_url + '/' + content_name + '/' + content_id

    req = requests.get(url)
    result = json.loads(req.content.decode())

    data_from_serive = result.get("data")

    if data_from_serive == '':
        return


    all_data = transform_request_data(data_from_serive)
    all_data = supply_up_count(all_data, user_id)

    out_data = clean_esdata(all_data)

    return out_data



def start_query(data):

    user_id = data.get("userId")
    try:
        data.pop('userId')
    except:
        pass

    test = query_subject(data)

    test = [(i, user_id) for i in test]

    res = list(map(query_subject_es, test))

    res = [i for i in res if i != None]

    out = {
        "status": "200",
        "code": "200",
        "viewTitle": "",
        "data": res}

    out_data = json.dumps(out)
    return out_data

