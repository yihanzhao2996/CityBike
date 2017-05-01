#!/usr/bin/env python

import socket
import pickle
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from threading import Thread
import time
import datetime
from geopy.distance import great_circle
import MySQLdb
from pyspark.mllib.clustering import GaussianMixture, GaussianMixtureModel

db = MySQLdb.connect("citibike.coyfwaoyaky7.us-east-2.rds.amazonaws.com", "YYY", "mypasswordyyy", "citibike")
cursor = db.cursor()
db.select_db('citibike')
#
cursor.execute('use citibike')
sc = SparkContext(appName="PythonStreamingQueueStream")
ssc = StreamingContext(sc, 5)
aFrame = ssc.socketTextStream("127.0.0.1", 6666)

accumulate_total = sc.parallelize([])
# (bikeId, (distance, duration, endTime, endStationId))
accumu_broken_bikes = sc.parallelize([])
# (bikeId, (endTime, endStationId, #))
geo_info_start = sc.parallelize([])
# ((timeSlot, stationId, 'out'), #)
geo_info_end = sc.parallelize([])
# ((timeSlot, stationId, 'in'), #)
geo_info = sc.parallelize([])
id_location = sc.parallelize([])
# (stationId, (stationGPS, stationName))
# id_loc = set()
route_pattern = sc.parallelize([])
# (time_slot, (start_la,start_lo,end_la,end_lo))

resultid = 0


def calculate(x):
    return 1.35 * great_circle((float(x[9]), float(x[10])), (float(x[5]), float(x[6]))).miles


def distance_rank(rdd):
    print 'distance rank called'
    global resultid
    global accumulate_total
    rdd = rdd.map(lambda x: x.split("^%"))
    rdd = rdd.map(lambda x: x + [calculate(x), calculate(x) / float(x[0])])
    print rdd.take(5)
    broken_detect(rdd)
    trip_info = rdd.map(lambda x: (x[11], (x[-2], int(x[0]), x[2], x[7])))
    # (bikeId, (distance, duration, endTime, endStationId))
    accumulate_total = accumulate_total.union(trip_info).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2], x[3]) \
        if datetime.datetime.strptime(x[2], "%Y-%m-%d %H:%M:%S") > datetime.datetime.strptime(y[2], "%Y-%m-%d %H:%M:%S") \
        else (x[0] + y[0], x[1] + y[1], y[2], y[3]))
    # print accumulate_total.take(5)

    top_50 = accumulate_total.top(50, key=lambda x: x[1][0])
    print
    print top_50
    # (bikeId, (distance, duration, endTime, endStationId))
    cursor.execute('delete from bike_usage')
    for item in top_50:
        print 'write to bike_usage'
        # print item
        cursor.execute(
            'insert into bike_usage (result_id, bike_id, acc_distance, acc_time, end_time, end_station_id) VALUES ("%s","%s","%s","%s","%s","%s")' % \
            (resultid, item[0], item[1][0], item[1][1], item[1][2], item[1][3]))
    db.commit()
    # print top_50[:4]
    resultid += 1


def broken_detect(rdd):
    global accumu_broken_bikes
    rdd = rdd.filter(lambda x: x[-2] == 0)
    broken_bikes = rdd.map(lambda x: (x[11], (x[2], x[7], 1)))
    # (bikeId, (endTime, endStationId, 1))
    accumu_broken_bikes = accumu_broken_bikes.union(broken_bikes).reduceByKey(lambda x, y: (x[0], x[1], x[2] + y[2]) \
        if datetime.datetime.strptime(x[0], "%Y-%m-%d %H:%M:%S") > datetime.datetime.strptime(y[0], "%Y-%m-%d %H:%M:%S") \
        else (y[0], y[1], x[2] + y[2]))
    # print accumu_broken_bikes.take(5)
    top_50 = accumu_broken_bikes.top(50, key=lambda x: x[1][-1])
    # (bikeId, (endTime, endStationId, #))
    cursor.execute('delete from broken_bikes')
    for item in top_50:
        print 'write to broken_bikes'
        # print item
        cursor.execute(
            'insert into broken_bikes (bike_id, last_station_id, last_time, zero_sum) VALUES ("%s","%s","%s","%s")' % \
            (item[0], item[1][1], item[1][0], item[1][2]))
    db.commit()
    # print top_50
    # (bikeId, (endTime, endStationId,  # ))
    #
    # print top_50[:4]


def heatmap_process(rdd):
    # aggregation by time and start/endStaionID
    global geo_info_start, geo_info_end, route_pattern, geo_info
    rdd = rdd.map(lambda x: x.split("^%"))
    route_pattern = rdd.map(lambda x: (datetime.datetime.strptime(x[1], "%Y-%m-%d %H:%M:%S").hour, (x[5], x[6], x[9], x[10])))
    rdd_start = rdd.map(lambda x: (datetime.datetime.strptime(x[1], "%Y-%m-%d %H:%M:%S").hour, x[3], 0))
    rdd_end = rdd.map(lambda x: (datetime.datetime.strptime(x[2], "%Y-%m-%d %H:%M:%S").hour, x[7], 1))
    rdd_start = rdd_start.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    rdd_end = rdd_end.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    geo_info_start = geo_info_start.union(rdd_start).reduceByKey(lambda x, y: x + y)
    geo_info_end = geo_info_end.union(rdd_end).reduceByKey(lambda x, y: x + y)
    geo_info = geo_info_start.union(geo_info_end)
    geo_ = rdd_start.union(rdd_end)
    # route_pattern = route_pattern.union(rdd_route)
    # ((timeSlot, stationId, 0/1), #)
    cursor.execute('delete from heatmap')
    for item in geo_info.collect():
        # for item in geo_.collect():
        print 'write to heatmap'
        cursor.execute(
            'insert into heatmap (time_slot, station_id, in_out, count) VALUES ("%s","%s","%s","%s")' % \
            (item[0][0], item[0][1], item[0][2], item[1]))
    db.commit()
    # clustering_data()


def id2location(rdd):
    global id_location
    rdd = rdd.map(lambda x: x.split("^%"))
    rdd1 = rdd.map(lambda x: (x[3], (x[5], x[6], x[4])))
    rdd2 = rdd.map(lambda x: (x[7], (x[9], x[10], x[8])))
    # rdd2 = rdd2.union(rdd1).groupByKey().mapValues(lambda x: list(x)[0])
    id_location = id_location.union(rdd1).groupByKey().mapValues(lambda x: list(x)[0])
    id_location = id_location.union(rdd2).groupByKey().mapValues(lambda x: list(x)[0])
    idl = id_location.collect()
    # (stationId, (stationGPS_la, stationGPS_ln, stationName))
    cursor.execute('delete from stations')
    for item in idl:
        print 'write to stations'
        cursor.execute('insert into stations (station_id,la,lo,address) VALUES ("%s","%s","%s","%s")' % \
                       (item[0], item[1][0], item[1][1], item[1][2]))
    db.commit()


# def clustering_data():
#     global route_pattern
#     for i in range(24):
#         print i
#         data_rdd = route_pattern.filter(lambda x: x[0] == i).map(lambda x: x[1])
#         print data_rdd
#         gmm = GaussianMixture.train(data_rdd, 2)
#         # output parameters of model
#         for i in range(2):
#             print("weight = ", gmm.weights[i], "mu = ", gmm.gaussians[i].mu,
#                   "sigma = ", gmm.gaussians[i].sigma.toArray())


aFrame.foreachRDD(distance_rank)
aFrame.foreachRDD(heatmap_process)
aFrame.foreachRDD(id2location)
# aFrame.foreachRDD()

ssc.start()
ssc.awaitTermination()
