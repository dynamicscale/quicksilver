import happybase
from subprocess import check_output,CalledProcessError,STDOUT
import time
import json
from celery_config import app

@app.task
def run_job(algoRowKey):
    hbaseConnection = happybase.Connection('localhost', '8025')
    algoTable = hbaseConnection.table('algos')
    
    algo = algoTable.row(algoRowKey)
    
    channelId, algoId = algoRowKey.split(':')
    
    baseAlgo = algo['info:base']
    
    actions = algo['info:actions']
    actions = json.loads(actions)
    actions = ','.join(actions)
    
    if 'info:sversion' in algo:
        sversion = int(algo['info:sversion'])
    else:
        sversion = 0

    newversion =  sversion+1
    newversion = str(newversion).zfill(6)
    
    currentStatus = 'notrunning'
    if 'info:status' in algo:
        currentStatus = algo['info:status']
    
    if currentStatus != 'running':
        
        '''
            == Start the spark job == 
        '''
        jobStartTime = int(time.time() * 1000)
        algoTable.put(algoRowKey, {'info:status': 'running', 'info:jobstart': str(jobStartTime)})
        
        jobOutcome = 'success'
        
        if baseAlgo == 'contentbased':
            jobCommand = "/usr/bin/python /home/vikas/Public/py/hbase/cbrunner.py "+channelId+" "+baseAlgo+" algoId="+algoId+" "+newversion;
        else:    
            jobCommand = "/usr/local/spark/bin/spark-submit --jars='/usr/local/elasticsearch-hadoop/dist/elasticsearch-hadoop-2.1.1.jar,/usr/local/spark/lib/spark-examples-1.4.0-hadoop2.6.0.jar' /home/vikas/Public/py/spark/sbase/sbase.py "+channelId+" "+baseAlgo+" actions="+actions+"#algoId="+algoId+" "+newversion;
        
        try:
            jobOutput = check_output(jobCommand, stderr=STDOUT, shell=True);
        except CalledProcessError as exc:
            jobOutcome = 'failure'
            jobOutput = exc.output
        
        jobFinishTime = int(time.time() * 1000)
        
        jobHistory = {'startTime': str(jobStartTime), 'endTime': str(jobFinishTime), 'job': jobCommand, 'outcome': jobOutcome, 'output': jobOutput}
        jobHistory = json.dumps(jobHistory)
        
        ## Update algo info in HBase
        
        algoUpdate = {'info:status': 'notrunning',
                      'info:lastoutcome': jobOutcome,
                      'jobhistory:'+str(jobFinishTime): jobHistory}
        
        if jobOutcome == 'success':
            algoUpdate['info:sversion'] = newversion
            algoUpdate['info:laststart'] = str(jobStartTime)
            algoUpdate['info:lastrun'] = str(jobFinishTime)
        
        algoTable.put(algoRowKey, algoUpdate)
        
@app.task
def process_bulk_upload(bulkUploadRowKey):
    hbaseConnection = happybase.Connection('localhost', '8025')
    
    bulkUploadTable = hbaseConnection.table('bulkuploads')
    channelTable = hbaseConnection.table('channels')
    
    channelId, timestamp, uploadId = bulkUploadRowKey.split(':')
    
    bulkUpload = bulkUploadTable.row(bulkUploadRowKey)
    
    if bulkUpload['info:processed'] == 'no':
    
        channel = channelTable.row(channelId)
        
        uploadPath = bulkUpload['info:path']
        clientId = channel['info:client']
        
        jobCommand = "/usr/local/spark/bin/spark-submit --jars='/usr/local/elasticsearch-hadoop/dist/elasticsearch-hadoop-2.1.1.jar,/usr/local/spark/lib/spark-examples-1.4.0-hadoop2.6.0.jar' /home/vikas/Public/py/spark/sbase/elasticload.py "+channelId+" "+clientId+" "+uploadId+" "+uploadPath
        
        jobOutcome = 'success'
        
        try:
            jobOutput = check_output(jobCommand, stderr=STDOUT, shell=True);
        except CalledProcessError as exc:
            jobOutcome = 'failure'
            jobOutput = exc.output
        
        print jobOutcome
        print jobOutput
        
        #bulkUploadTable.put(bulkUploadRowKey, {'info:processed': 'yes'})