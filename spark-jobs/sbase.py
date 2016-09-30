from __future__ import division
from __future__ import print_function
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import math
import sys
import json

'''
 Get itemid, userid pairs from input data set
''' 
def getItemUserPairs(line):
    userid = line[1]['userId']    
    itemid = line[1]['itemId']
    return (itemid,userid)

def getUserItemRatingPairs(line):
    userid = line[1]['userId']    
    itemid = line[1]['itemId']
    rating = float(line[1]['rating'])
    return (userid, (itemid, rating))

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

'''
 Save similarities in HBase
 Input scores in format: (item1, item2, score)
'''
def saveInHBase(scores, channelId, version, algo):
    '''
     Since similarities are bi-directional
     From (item1, item2, score) we get two tuples:
        - (item1, item2, score)
        - (item2, item1, score)
    '''
    
    rowKeyPrefix = channelId.lower()+':'+algo.lower()+':'
    
    allScores = scores.flatMap(lambda x: ((rowKeyPrefix+str(x[0])+':'+version, str(x[1]), str(x[2])),(rowKeyPrefix+str(x[1])+':'+version, str(x[0]), str(x[2]))))

    '''
     Convert to HBase data
     (rowkey, [rowkey, columnfamily, column, value])
        - rowkey is itemid
        - columnfamily is 's'
        - column is score:itemid (score is 0 padded for sorting)
        - value is 1 (not used)
    '''     
    hbaseData = allScores.map(lambda x : (x[0], [x[0], 's', str(x[2]).zfill(6)+":"+str(x[1]), '1']))
    
    hbaseConf = getHBaseConfig('localhost', 's1.similarity')
    hbaseData.saveAsNewAPIHadoopDataset(conf=hbaseConf['conf'],keyConverter=hbaseConf['keyConverter'],valueConverter=hbaseConf['valueConverter'])

'''
 Save user recommendations in HBase
 Input scores in format: (userid, itemid, score)
'''
def saveUserRecommendationsInHBase(scores, channelId, version, algo):
    
    rowKeyPrefix = channelId.lower()+':'+algo.lower()+':'
    
    '''
     Convert to HBase data
     (rowkey, [rowkey, columnfamily, column, value])
        - rowkey is channelId:algoId:userId
        - columnfamily is 'r'
        - column is itemid
        - value is score
    '''     
    hbaseData = scores.map(lambda x : (rowKeyPrefix+str(x[0])+':'+version, [rowKeyPrefix+str(x[0])+':'+version, 'r', str(x[1]), str(x[2])]))
    
    hbaseConf = getHBaseConfig('localhost', 's1.recommendations')
    hbaseData.saveAsNewAPIHadoopDataset(conf=hbaseConf['conf'],keyConverter=hbaseConf['keyConverter'],valueConverter=hbaseConf['valueConverter'])

'''
 Compute jaccard similarity
''' 
def computeJaccard(val):
    itemPair = val[0]
    item1 = itemPair[0]
    item2 = itemPair[1]
    occurrencesTogether = val[1][1]
    item1Occurrences = val[1][0][0]
    item2Occurrences = val[1][0][1]
    
    jaccard = occurrencesTogether/(item1Occurrences + item2Occurrences - occurrencesTogether)
    jaccard = int(round(jaccard * 1000));
    return (item1, item2, jaccard)

'''
 Compute cosine similarity
'''
def computeCosine(val):
    itemPair = val[0]
    item1 = itemPair[0]
    item2 = itemPair[1]
    occurrencesTogether = val[1][1]
    item1Occurrences = val[1][0][0]
    item2Occurrences = val[1][0][1]
    
    cosine = occurrencesTogether/(math.sqrt(item1Occurrences) + math.sqrt(item2Occurrences))
    cosine = int(round(cosine * 1000));
    return (item1, item2, cosine)

'''
 Compute log likelihood similarity
'''
def computeLogLikelihood(val):
    itemPair = val[0]
    item1 = itemPair[0]
    item2 = itemPair[1]

    item1Occurrences = val[1][0][0]
    item2Occurrences = val[1][0][1]
    
    totalOccurrences = numDataPoints
    occurrencesTogether = val[1][1]
    item1WithoutItem2 = item1Occurrences - occurrencesTogether
    item2WithoutItem1 = item2Occurrences - occurrencesTogether
    
    if occurrencesTogether <= 0:
        return (item1, item2, 0)
    
    rowEntropy = entropy([occurrencesTogether + item2WithoutItem1, item1WithoutItem2 + totalOccurrences])
    columnEntropy = entropy([occurrencesTogether + item1WithoutItem2, item2WithoutItem1 + totalOccurrences])
    matrixEntropy = entropy([occurrencesTogether, item2WithoutItem1, item1WithoutItem2, totalOccurrences])
    
    if rowEntropy + columnEntropy < matrixEntropy:
        return (item1, item2, 0)
    
    LLScore = 2 * (rowEntropy + columnEntropy - matrixEntropy)
    LLScore = int(round(LLScore * 1000));
    return (item1, item2, LLScore)
    
def entropy(elements):
    total = 0
    result = 0
    
    for element in elements:
        result += xLogx(element)
        total += element
    
    return xLogx(total) - result

def xLogx(value):
    if value == 0 :
        return 0
    else:
        return value * math.log(value)

def pearson(val):
    itemPair = val[0]
    item1 = itemPair[0]
    item2 = itemPair[1]
    
    values = val[1]
    numValues = len(values)
    
    sum1 = 0.0
    sum2 = 0.0
    sum1Sq = 0.0
    sum2Sq = 0.0
    psum = 0.0
    
    for valuePair in values:
        value1 = valuePair[0]
        value2 = valuePair[1]
        
        sum1 += float(value1)
        sum2 += float(value2)
        sum1Sq += float(value1)**2
        sum2Sq += float(value2)**2
        psum += float(value1) * float(value2)
        
    num = psum-(sum1*sum2/numValues)
    den = math.sqrt((sum1Sq-pow(sum1,2)/numValues)*(sum2Sq-pow(sum2,2)/numValues))
    
    if den==0:
        pearsonScore = 0
    else:
        pearsonScore = (1 + num/den)/2
        pearsonScore = int(round(pearsonScore * 1000));
    
    return (item1, item2, pearsonScore)

if __name__ == "__main__":
    
    if len(sys.argv) < 5:
        print("Usage: sbase.py <channelId> <algo> <algoParams> <version>")
        exit(-1)
    
    channelId = sys.argv[1]
    algo = sys.argv[2]
    algoParamsArg = sys.argv[3]
    version = sys.argv[4]
    
    ''' Extract command line algo params '''
    algoParamList = algoParamsArg.split('#')
    algoParams = {}
    for param in algoParamList:
        paramKey, paramValue = param.split('=')
        paramValue = paramValue.strip()
        if paramValue:
            if paramKey == 'actions':
                paramValue = paramValue.split(',')
            algoParams[paramKey] = paramValue    
    
    algoId = algoParams['algoId']
    
    ''' Load Spark context '''
    sc = SparkContext("local", "Simple App")

    dataQuery = {
                    "query": {
                        "filtered": {
                            "filter": {
                                "bool": {
                                    "must": []
                                }
                            }
                        }
                    }
                }

    dataQuery["query"]["filtered"]["filter"]["bool"]["must"].append({"term": {"channelId.raw" : channelId}})
    
    if algo == 'pearson' or algo == 'als' or algo == 'alsimplicit':
        dataQuery["query"]["filtered"]["filter"]["bool"]["must"].append({"term": {"action" : 'rating'}})
    
    if 'actions' in algoParams:
        dataQuery["query"]["filtered"]["filter"]["bool"]["must"].append({"terms": {"action" : algoParams['actions']}})    

    
    ''' Load data from elasticsearch index '''
    lines = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
                               keyClass="org.apache.hadoop.io.NullWritable",
                               valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                               conf={
                                        "es.resource" : "c6_channel/data",
                                        "es.query" : json.dumps(dataQuery)
                                    }
                            )
    
    lines.cache()
    
    if algo == 'pearson':
        ''' Extract (userid, itemid, rating) pairs from input '''
        userItemRatingPairs = lines.map(getUserItemRatingPairs).distinct()
        clonedUserItemRatingPairs = userItemRatingPairs.map(lambda x: x)
        
        ''' Join the two datasets '''
        joinedUserItemRatingPairs = userItemRatingPairs.join(clonedUserItemRatingPairs)
        
        ''' Deduplicate '''
        deduplocatedJoinedUserItemRatingPairs = joinedUserItemRatingPairs.filter(lambda x: x[1][0][0] < x[1][1][0])
        
        '''
        Get item pairs
        (item1, item2), (rating for item1 by a user, rating for item2 by the same user)
        '''    
        itemPairs = deduplocatedJoinedUserItemRatingPairs.map(lambda x: ((x[1][0][0], x[1][1][0]), (x[1][0][1], x[1][1][1])))
        
        ''' Group by item pairs '''
        groupedItemPairs = itemPairs.groupByKey().map(lambda x: (x[0], list(x[1])))
        
        pearsonScores = groupedItemPairs.map(pearson)
        saveInHBase(pearsonScores, channelId, version, algoId)
    elif algo == 'als' or algo == 'alsimplicit':
        ''' Extract (userid, itemid, rating) pairs from input '''
        userItemRatingPairs = lines.map(getUserItemRatingPairs).distinct()
        ratings = userItemRatingPairs.map(lambda l: Rating(int(l[0]), int(l[1][0]), float(l[1][1])))
        
        rank = 10
        numIterations = 10
        
        if algo == 'alsimplicit':
            model = ALS.trainImplicit(ratings, rank, numIterations, alpha=0.01)
        else:    
            model = ALS.train(ratings, rank, numIterations)
            
        ratingdata = ratings.map(lambda p: (p[0], p[1]))
        predictions = model.predictAll(ratingdata).map(lambda r: (r[0], r[1], r[2]))
        saveUserRecommendationsInHBase(predictions, channelId, version, algoId)
    else :
        ''' Extract (itemid, userid) pairs from input '''
        itemUserPairs = lines.map(getItemUserPairs).distinct()
        
        numDataPoints = itemUserPairs.count()
        
        ''' Get all items along with their occurrences '''
        itemsWithCounts = itemUserPairs.map(lambda x: (x[0], 1)).reduceByKey(lambda a,b:a+b)
        
        ''' Join two sets to get (itemid, userid, numItemOccurrence) set ''' 
        itemUserPairsWithCounts = itemUserPairs.join(itemsWithCounts)
        
        '''
         Convert the set to (userid, (itemid, numItemOccurrence))
         so that it is keyed by userid
        ''' 
        userItemPairsWithCounts = itemUserPairsWithCounts.map(lambda x: (x[1][0], (x[0], x[1][1])))
        
        ''' Clone RDD so that we can join '''
        cloneUserItemPairsWithCounts = userItemPairsWithCounts.map(lambda x: x)
        
        ''' Join the two on userid '''
        joinedSet = userItemPairsWithCounts.join(cloneUserItemPairsWithCounts)
        
        ''' Filter duplicates '''
        deduplicatedJoinedSet = joinedSet.filter(lambda x : x[1][1][0] > x[1][0][0])
        
        '''
         Create item pairs
         Format: (item1, item2), ((item1_occurrences, item2_occurrences), 1)
         Added 1 so that we can count total occurrences of (item1, item2) together
        ''' 
        itemPairs = deduplicatedJoinedSet.map(lambda x: ((x[1][0][0], x[1][1][0]), ((x[1][0][1],x[1][1][1]), 1)))
        
        '''
         Group item pairs
         We get: (item1, item2), ((item1_occurrences, item2_occurrences), item1_item2_occurrences)
        '''
        groupedItemPairs = itemPairs.reduceByKey(lambda a,b: (a[0], a[1]+b[1]))
        
        if algo == 'loglikelihood':
            '''
             Log Likelihood Ratio
            '''
            logLikelihoodScores = groupedItemPairs.map(computeLogLikelihood)
            saveInHBase(logLikelihoodScores, channelId, version, algoId)
        
        if algo == 'cooccurrence':
            '''
             Co-occurrence algo
            ''' 
            cooccurrenceScores = groupedItemPairs.map(lambda x: (x[0][0], x[0][1], x[1][1]))
            saveInHBase(cooccurrenceScores, channelId, version, algoId)
        
        if algo == 'jaccard':
            '''
             Jaccard algo
            ''' 
            jaccardScores = groupedItemPairs.map(computeJaccard)
            saveInHBase(jaccardScores, channelId, version, algoId)
        
        if algo == 'cosine':
            '''
             Cosine algo
            ''' 
            cosineScores = groupedItemPairs.map(computeCosine)
            saveInHBase(cosineScores, channelId, version, algoId)