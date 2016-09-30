from __future__ import print_function

import sys
import time
import calendar

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    
    if len(sys.argv) != 3:
        print("Usage: wc.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="QuickSilverStreamingKafkaProcessing")
    ssc = StreamingContext(sc, 1)

    hbaseConf = {
        "hbase.zookeeper.quorum": "localhost",
        "hbase.mapred.outputtable": "c1.users",
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
    }

    keyConverter = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConverter = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    
    # Fetch messages from kafka
    lines = kvs.map(lambda x: x[1])
    
    # Convert lines to tuples
    # Each message is a tuple <clientId, channelId, userId, itemId, time, action>
    datapoints = lines.map(lambda line: line.split(","))
    
    #
    ### HBASE
    #
    
    # Get user-item interactions into HBase row format
    userItemInteractionDataHBase = datapoints.map(lambda x: (x[1]+":"+x[2], [x[1]+":"+x[2], 'i', str(calendar.timegm(time.strptime(x[4], '%Y-%m-%d %H:%M:%S')))+":"+x[3], x[5]]));
    
    # Save data into HBase users table 
    userItemInteractionDataHBase.foreachRDD(lambda userItemInteractionRDD: userItemInteractionRDD.saveAsNewAPIHadoopDataset(conf=hbaseConf,keyConverter=keyConverter,valueConverter=valueConverter))

    #
    ### ELASTICSEARCH
    #

    # Get user-item interactions into ElasticSearch document format
    userItemInteractionDataElastic = datapoints.map(lambda x: ('key', {'clientId' : x[0], 'channelId': x[1], 'ingestion': 'stream', 'userId': int(x[2]), 'itemId': int(x[3]), 'time': x[4].replace(' ','T'), 'action': x[5]}))
    
    userItemInteractionDataElastic.foreachRDD(lambda userItemInteractionRDD: userItemInteractionRDD.saveAsNewAPIHadoopFile(path='-', outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", keyClass="org.apache.hadoop.io.NullWritable", valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", conf={ "es.resource" : 'c6_channel/data' }))

    ssc.start()
    ssc.awaitTermination()