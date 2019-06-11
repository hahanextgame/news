# -*- coding: utf-8 -*-

# @Time :2018/12/12 14:52
# @File: streamRule.py
# @目的

from pykafka import KafkaClient
import json
import requests
import redis
import ReSysApi2.configs as config


def hander_rule(mess):

    try:
        text = json.loads(mess[30:].decode())

        method = text.get("method")
        url = text.get("service")
        id = text.get("contentId")

        data = {"productCode": id}
        a = requests.post(url, json=data)
        info = json.loads(a.content)

        # --得到相关的消息
        rule = info['data']['ruleInfoDtoList']
        ids = [(i['ruleName']) for i in rule]
        values = [i['ruleValue'] for i in rule]
        dict_data_1 = dict(zip(ids, values))

        # --进行名称和contentType的映射--
        dict_data = {}
        dict_data[1] = dict_data_1['图文']
        dict_data[2] = dict_data_1['小视频']
        dict_data[3] = dict_data_1['新鲜事']
        dict_data[4] = dict_data_1['活动']
        dict_data[6] = dict_data_1['专题']

        #dict_data[77] = dict_data_1['专题置顶']
        dict_data[55] = dict_data_1['置顶']
        dict_data[66] = dict_data_1['快讯']
        dict_data[111] = dict_data_1['升级热点']

        rdp = redis.ConnectionPool(host=config.redis_ip, port=config.redis_port, db=config.redis_db, password=config.redis_pass)
        red = redis.StrictRedis(connection_pool=rdp)
        red.zremrangebyscore('rule', 1, 1)
        red.zadd('rule', 1, json.dumps(dict_data))
    except Exception as e:
        print(e)



if __name__ == '__main__':
    z = '192.168.12.202:2181'
    client = KafkaClient(hosts=config.kafka_config, zookeeper_hosts=config.zookeep_config)

    topic = client.topics[b'haike-stream-rule']
    consumer = topic.get_simple_consumer()

    for message in consumer:
        if message is not None:
            print(message.offset, message.value)
            test = message.value
            hander_rule(test)

