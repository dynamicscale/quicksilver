from __future__ import division
from __future__ import print_function
from pyspark import SparkContext
import sys
import time
import calendar

'''
 Get HBase config
'''     
def getHBaseConfig(hostname, tablename):
    conf = {
        "hbase.zookeeper.quorum": hostname,
        "hbase.mapred.outputtable": tablename,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
    }
    
    keyConverter = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConverter = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    
    return {"conf": conf, "keyConverter": keyConverter, "valueConverter": valueConverter}

def convertToHBaseData(datatuple):
    
    activityUserId = int(datatuple[0])
    activityItemId = int(datatuple[1])
    
    dataRowKey = channelId+":"+str(activityUserId)
    
    activityTime = datatuple[2]
    activityAction = datatuple[3]
    dataColumn = str(calendar.timegm(time.strptime(activityTime, '%Y-%m-%d %H:%M:%S')))+":"+str(activityItemId)
    
    dataValue = activityAction
    if len(datatuple) == 5:
        dataValue = activityAction+":"+str(datatuple[4])
    
    return (dataRowKey, [dataRowKey, "i", dataColumn, dataValue])

'''
 Save user interactions in HBase
 Format: (userid, itemid, datetime, action)
'''
def saveInHBase(tuples, tablename):
    '''
     Convert to HBase data
     (rowkey, [rowkey, columnfamily, column, value])
        - rowkey is userid
        - columnfamily is 'i'
        - column is timestamp:itemid
        - value is action
    '''     
    hbaseData = tuples.map(convertToHBaseData)
    
    hbaseConf = getHBaseConfig('localhost', tablename)
    hbaseData.saveAsNewAPIHadoopDataset(conf=hbaseConf['conf'],keyConverter=hbaseConf['keyConverter'],valueConverter=hbaseConf['valueConverter'])

def convertToElasticData(datatuple):
    esData = {
                'clientId' : clientId,
                'channelId': channelId,
                'ingestion': 'bulk',
                'bulkfile': uploadId,
                'userId': int(datatuple[0]),
                'itemId': int(datatuple[1]),
                'time': datatuple[2].replace(' ','T'),
                'action': datatuple[3]
            }
    
    if len(datatuple) == 5:
        esData['rating'] = int(datatuple[4])
    
    return ('key', esData)

'''
 Save documents in ElasticSearch
 Format: (userid, itemid, datetime, action)
'''
def saveInElasticSearch(tuples, indexname):
    '''
     Convert to ElasticSearch data
    '''     
    elasticData = tuples.map(convertToElasticData)
   
    elasticData.saveAsNewAPIHadoopFile(path='-', outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", keyClass="org.apache.hadoop.io.NullWritable", valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", conf={ "es.resource" : indexname })


if __name__ == "__main__":

    if len(sys.argv) < 5:
        print("Usage: elasticload.py <channelId> <clientId> <uploadId> <uploadPath>")
        exit(-1)
    
    channelId = sys.argv[1]
    clientId = sys.argv[2]
    uploadId = sys.argv[3]
    uploadPath = sys.argv[4]

    sc = SparkContext("local", "Simple App")

    ''' Load input file '''    
    lines = sc.textFile('hdfs://localhost:8080'+uploadPath).cache()

    ''' Extract (userid, itemid, datetime, action) tuples from input '''
    tuples = lines.map(lambda x: x.split(','))
    saveInElasticSearch(tuples, 'c6_channel/data')
    saveInHBase(tuples, 'c1.users')