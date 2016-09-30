import redis
from jobrunner import run_job, process_bulk_upload

config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
}

r = redis.StrictRedis(**config)

if __name__ == '__main__':
    channel = 'JobsToRun'

    pubsub = r.pubsub()
    pubsub.subscribe(channel)

    print 'Listening to {channel}'.format(**locals())

    while True:
        for item in pubsub.listen():
            if item['type'] == 'message':
                data = item['data']
                jobIdentifier, jobData = data.split('#')
                
                print jobIdentifier
                print jobData
                
                if jobIdentifier == 'PROCESSALGO':
                    run_job.delay(jobData)
                else:
                    process_bulk_upload.delay(jobData)
