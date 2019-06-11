# -*- coding: utf-8 -*-

# @Time :2019/2/27 10:24
# @File: testData.py
# @目的: 相关的工具


import requests



def query_last_fresh(ids, related_url=""):
    try:
        para_data = {
            "relatednewsId": ids
        }

        response_object = requests.get(related_url, params=para_data, timeout=1)
        response_data = response_object.json()

        last_fresh = response_data.get('data', '')

        last_fresh = "" if last_fresh == None else last_fresh

        return last_fresh
    except Exception as e:
        print("fresh_news", e)
        return ""




def black_query(mediaId):
    try:
        base_url = ""
        request_url = base_url + str(mediaId)

        response_data = requests.get(request_url)
        response_data = response_data.json()

        black_media_id = response_data.get('data')

        if black_media_id == "":
            black_media_id = []

        return black_media_id
    except Exception as e:
        pass
        return []


def query_special_info(ids, base_url=""):
    try:
        request_url = base_url + str(ids)
        response_data = requests.get(request_url, timeout=1)

        response_data = response_data.json().get('data')
        if response_data == "":
            return ""

        related_ids = response_data[0]

        es_url = "" + str(related_ids)

        requests_es = requests.get(es_url)
        es_data = requests_es.json()

        out_data = es_data.get('data')
        return out_data
    except Exception as e:
        print("query_special_info", e)
        return ""

def supply_up_count(datafast, media_id):

    contentType = str(datafast.get("contentType"))  

    if contentType not in ['3', '9']:
        return datafast

    if contentType=="3":
        contentId = str(datafast.get('freshnewsId'))
    else:
        contentId = str(datafast.get("mid"))

    if contentType == "3":
        url_newthings = ''
        if media_id:
            data = {
                'unq_id': str(contentId) + '_newthings',
                'media_id': media_id
            }
        else:
            data = {'unq_id': str(contentId) + '_newthings'}

        response_data = requests.get(url_newthings, params=data)
        response_data = response_data.json()

    elif contentType=='9':
        # --短视频
        url_video = ''
        url_video = url_video + str(contentId)

        if media_id:
            data = {'media_id': media_id}
            response_data = requests.get(url_video, params=data)
            response_data = response_data.json()
        else:
            response_data = requests.get(url_video)
            response_data = response_data.json()


    if response_data.get('data') != None:

        source_data = response_data.get('data').get('item')

        datafast['user_is_up'] = source_data.get('user_is_up')

        datafast['up_count'] = source_data.get('up_count')

        datafast['comment_count'] = source_data.get('comment_count')
        return datafast
    else:
        return datafast




