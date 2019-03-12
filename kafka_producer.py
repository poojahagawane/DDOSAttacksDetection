# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 21:55:16 2019

@author: pooja hagavane
"""


from kafka import KafkaProducer
import re
import json

#local host: 127.0.0.1:9092
producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])

#input file: apache-access-log.txt
with open('apache-access-log.txt','r') as lines:
    for line in lines:
        remote_ip = map(''.join, re.findall(r'\"(.*?)\"|\[(.*?)\]|(\S+)', line))
        producer.send('messages', json.dumps({'remote_host': remote_ip[0]}))