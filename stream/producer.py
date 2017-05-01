#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import sys
import pickle
import socket
import time
from os import walk
import re
import heapq
import datetime
# from pyspark.streaming.kafka import KafkaUtils

# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext

TCP_IP = '127.0.0.1'
TCP_PORT = 6666
BUFFER_SIZE = 4096

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

conn, addr = s.accept()
# print 'Connection address:', addr


def walker():
    db_path = 'data/'
    f = []
    pq = []
    base_time = datetime.datetime(2017,1,1)
    print base_time
    for (dirpath, dirnames, filenames) in walk(db_path):
        for filename in filenames:
            print filename
            mt_obj = re.search(r'.*.csv\b', filename)
            if mt_obj:
                time_str = filename.split('-')[0]
                time_time = datetime.datetime.strptime(time_str, "%Y%m")
                heapq.heappush(pq,((time_time-base_time).total_seconds(), filename))
                f.extend([filename])
    print f
    print len(f)
    print pq
    return pq


def unzip(base_time, file_name):
    # print file_name
    csvreader = csv.reader(open(file_name, 'r'))
    next(csvreader)
    # basetime = datetime.datetime(2017,1,1,0,0,0)
    print base_time
    Message = ''
    for row in csvreader:
        print (datetime.datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S") - base_time).total_seconds()
        if (datetime.datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S") - base_time).total_seconds() <= 60:
            Message += '^%'.join(row) + '\n'
        else:
            print '===='
            print Message
            print '***'
            conn.send(Message)
            Message = ''
            base_time = datetime.datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
            Message += '^%'.join(row) + '\n'
            # base_time += datetime.timedelta(seconds=60)
            print base_time
            # print Message
            time.sleep(0.01)


if __name__ == '__main__':
    pq = walker()
    basetime = datetime.datetime(2017, 1, 1, 0, 0, 0)
    for name in pq:
        path_name = 'data/'+name[1]
        data = unzip(basetime, path_name)
