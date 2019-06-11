# -*- coding: utf-8 -*-

# @Time :2018/12/11 10:13
# @File: Api.py
# @目的
from flask import Flask, request, abort, Response
import json
import flask_profiler

from gevent import monkey
from gevent.pywsgi import WSGIServer

from ReSysApi2.Rules import compute_lonlat, compute_contrycode, del_es_his, compute_lonlat_haikehao
from ReSysApi2.Rules import ReHist, ReHist_haikehao
from ReSysApi2.trans import start_query


monkey.patch_all()

app = Flask(__name__)
app.config["DEBUG"] = False


app.config["flask_profiler"] = {
    "enabled": app.config["DEBUG"],
    "storage": {
        "engine": "sqlite"
    },
    "basicAuth":{
        "enabled": True,
        "username": "admin",
        "password": "admin"
    },
    "ignore": [
	    "^/static/.*"
	]
}


@app.route('/Resys', methods=['POST'])
def Resys():
    try:
        id = request.args.get('userId')
        mediaId = request.args.get("mediaId")
        souceType = request.args.get("souceType")
        lon = request.args.get("longitude")
        lat = request.args.get("latitude")
        mid = request.args.get("imei")
        productCode = request.args.get("productCode")
        version = request.args.get("version")
        country_code = request.args.get('countryCode')

        # country_code = None
        if souceType:
            # 这个是海客号的
            print('-')
            # if country_code:
            #     temp = compute_contrycode_haikehao(id, country_code, mid)
            #     return Response(temp, mimetype='application/json')
            # else:
            #     temp = compute_lonlat_haikehao(id, lon, lat, mid)
            #     return Response(temp, mimetype='application/json')
            temp = compute_lonlat_haikehao(id, lon, lat, mid, mediaId, version)
            return Response(temp, mimetype='application/json')
        else:
            if country_code:
                temp = compute_contrycode(id, mid, lon, lat, mediaId, version)
                return Response(temp, mimetype='application/json')
            else:
                print('---')
                temp = compute_lonlat(id, lon, lat, mid, mediaId, version)
                return Response(temp, mimetype='application/json')
            # temp = compute_lonlat(id, lon, lat, mid)
            return Response(temp, mimetype='application/json')
    except:
            abort(404)


@app.route('/ResysHist', methods=['POST'])
def ResysHist():
    try:
        current = request.args.get('current')
        size = request.args.get('size')
        id = request.args.get('userId')
        mid = request.args.get("imei")
        souceType = request.args.get("souceType")
        lon = request.args.get('longitude')
        lat = request.args.get('latitude')
        mediaId = request.args.get("mediaId")

        productCode = request.args.get("productCode")
        version = request.args.get("version")
        country_code = request.args.get('countryCode')

        if souceType:
            temp = ReHist_haikehao(id, mid, current, size, lon, lat, mediaId, version)
            return Response(temp, mimetype='application/json')
        else:
            print('mid')
            print(id) 
            print(lon)
            temp = ReHist(id, mid, current, size, lon, lat, mediaId, version)
            return Response(temp, mimetype='application/json')
    except:
        abort(404)


@app.route('/')
def hello_world():
    return 'Hello World!'



@app.route('/subject_query', methods=['POST'])
def subject_query():
    try:
        data = json.loads(request.get_data(as_text=True))
        print(data)
        res = start_query(data)
        return Response(res, mimetype='application/json')
    except Exception as e:
        print(e)
        abort(404)



@app.route('/del_history', methods=['POST'])
def del_history():
    try:
        content_id = request.args.get('contentId')
        res = del_es_his(content_id)
        return Response(res, mimetype='application/json')
    except Exception as e:
        abort(404)




flask_profiler.init_app(app)



if __name__ == '__main__':
    http_server = WSGIServer(('0.0.0.0', 5000), app)
    http_server.serve_forever()