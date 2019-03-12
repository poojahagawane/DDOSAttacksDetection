# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 22:31:36 2019

@author: pooja hagavane
"""


import time
from datetime import datetime
import json
from pyspark import SparkContext , SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#function to update ip address hits
def update_function(new_values, running_count):
    if running_count is None:
       running_count = 0
    return sum(new_values, running_count) 

#function to extract ip address from message produced by KafkaProducer
def get_ip_address(msg):
    message = json.loads(msg[1])
    return message['remote_host'], 1

#main function
if __name__ == '__main__':
    start=datetime.now()
    conf = (SparkConf().setMaster("local").setAppName("My app to find DDOS attacks").set("spark.executor.memory", "1g"))
    sc = SparkContext(conf = conf)
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('checkpoint')
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    directKafkaStream = KafkaUtils.createDirectStream(ssc, ['messages'], {"metadata.broker.list": '127.0.0.1:9092'})
    ip_count_pair = directKafkaStream.map(get_ip_address)  
    ip_counts = ip_count_pair.updateStateByKey(update_function)
    update_running_counts = ip_counts.map(lambda (key,value): (str(key), value))
    attackers = update_running_counts.filter(lambda (key,value): value >= 80)
    attackers.saveAsTextFiles('Attacker_ip_addresses')
    ssc.start()
    time.sleep(60)
    ssc.stop()