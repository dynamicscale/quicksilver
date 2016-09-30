var express = require('express');
var hbase = require('hbase');
var kafka = require('kafka-node');
var util = require('util');
var moment = require('moment');
var async = require("async");
var WebHDFS = require('webhdfs');
var elasticsearch = require('elasticsearch');
var trim = require('trim');
var redis = require("redis");
var sleep = require('sleep');
var murmurhash = require('node-murmurhash');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res) {
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientTable = hclient.table('clients');
    var channelTable = hclient.table('channels');
    var search = req.query.s;
    
    clientTable.row('CL-*').get(function(error, data) {
        results = {};
        data.forEach(function(d) {
            clientId = d.key.toString();
            if (!(clientId in results)) {
                results[clientId] = {};
            }
            columnData = d.column.toString().split(':');
            if (columnData[0] == 'info') {
                columnName = columnData[1];
                columnVal = d.$.toString();
                if (columnName == 'created') {
                    columnVal = moment(parseInt(columnVal)).format('D MMMM, YYYY');
                }
                if (search && columnName == 'name') {
                    if (columnVal.toLowerCase().search(search.toLowerCase()) != -1) {
                        results[clientId][columnName] = columnVal;
                    }
                }
                else {
                    results[clientId][columnName] = columnVal;
                }
            }
        });
        
        channelTable.row('CH-*').get(function(error, data) {
            channelResults = {};
            data.forEach(function(d) {
                channelId = d.key.toString();
                if (!(channelId in channelResults)) {
                    channelResults[channelId] = {};
                }
                columnData = d.column.toString().split(':');
                
                cf = columnData[0];
                if (!(cf in channelResults[channelId])) {
                    channelResults[channelId][cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();
                channelResults[channelId][cf][columnName] = columnVal;
            });
            
            for(channelId in channelResults) {
                channelClient = channelResults[channelId]['info']['client'];
                if(!('channels' in results[channelClient])) {
                    results[channelClient]['channels'] = {}
                }
                results[channelClient]['channels'][channelId] = channelResults[channelId];
            }
            
            var clientsWithChannels = {}
            var clientsWithoutChannels = {}
            
            for(clientId in results) {
                if (results[clientId]['channels']) {
                    clientsWithChannels[clientId] = results[clientId]
                }
                else {
                    clientsWithoutChannels[clientId] = results[clientId]
                }
            }
            
            results = {}
            for (clientId in clientsWithChannels) {
                results[clientId] = clientsWithChannels[clientId]
            }
            for (clientId in clientsWithoutChannels) {
                results[clientId] = clientsWithoutChannels[clientId]
            }
            
            res.render('index',  { clients: results, num: Object.keys(results).length, search: search, 'hideheader': true, moment: moment })  
            
        })
    });
});

router.get('/bigMenu', function(req, res) {
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientTable = hclient.table('clients');
    var channelTable = hclient.table('channels');
    var selectedClientId = req.query.client;
    //sleep.sleep(1)
    clientTable.row('CL-*').get(function(error, data) {
        results = {};
        data.forEach(function(d) {
            clientId = d.key.toString();
            if (!(clientId in results)) {
                results[clientId] = {};
            }
            columnData = d.column.toString().split(':');
            if (columnData[0] == 'info') {
                columnName = columnData[1];
                columnVal = d.$.toString();
                if (columnName == 'created') {
                    columnVal = moment(parseInt(columnVal)).format('D MMMM, YYYY');
                }
                results[clientId][columnName] = columnVal;
            }
        });
        
        channelTable.row('CH-*').get(function(error, data) {
            channelResults = {};
            data.forEach(function(d) {
                channelId = d.key.toString();
                if (!(channelId in channelResults)) {
                    channelResults[channelId] = {};
                }
                columnData = d.column.toString().split(':');
                
                cf = columnData[0];
                if (!(cf in channelResults[channelId])) {
                    channelResults[channelId][cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();
                channelResults[channelId][cf][columnName] = columnVal;
            });
            
            for(channelId in channelResults) {
                channelClient = channelResults[channelId]['info']['client'];
                if(!('channels' in results[channelClient])) {
                    results[channelClient]['channels'] = {}
                }
                results[channelClient]['channels'][channelId] = channelResults[channelId];
            }
            
            var clientsWithChannels = {}
            var clientsWithoutChannels = {}
            
            for(clientId in results) {
                if (results[clientId]['channels']) {
                    clientsWithChannels[clientId] = results[clientId]
                }
                else {
                    clientsWithoutChannels[clientId] = results[clientId]
                }
            }
            
            results = {}
            for (clientId in clientsWithChannels) {
                results[clientId] = clientsWithChannels[clientId]
            }
            for (clientId in clientsWithoutChannels) {
                results[clientId] = clientsWithoutChannels[clientId]
            }
            console.log(results)
            res.render('bigMenu',  { clients: results, num: Object.keys(results).length, 'hideheader': true, moment: moment, selectedClientId: selectedClientId })  
            
        })
    });
});

router.get('/about', function(req, res) {
    res.render('about',  {'hideheader': true})  
})

router.get('/algos', function(req, res) {
    res.render('algos',  {'hideheader': true})  
})

router.get('/contact', function(req, res) {
    res.render('contact',  {'hideheader': true})  
})

router.get('/client', function(req, res) {
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientTable = hclient.table('clients');
    var channelTable = hclient.table('channels');
    var clientId = req.query.id;
    
    clientTable.row(clientId).get(function(error, data) {
        if (data) {
            results = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in results)) {
                    results[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();
                results[cf][columnName] = columnVal;
            });
            
            if (results.channels) {
                channels = Object.keys(results.channels).reverse();
                channelResults = []
                
                async.each(channels, function(channel, callback){
                        channelTable.row(channel).get(function(error, data) {
                            chresults = {};
                            createdTimestamp = 0;
                            data.forEach(function(d) {
                                columnData = d.column.toString().split(':');
                                cf = columnData[0];
                                if (!(cf in chresults)) {
                                    chresults[cf] = {};
                                }
                                
                                columnName = columnData[1];
                                columnVal = d.$.toString();
                                
                                if (columnName == 'created') {
                                    createdTimestamp = parseInt(columnVal);
                                    columnVal = moment(parseInt(columnVal)).format('D MMMM, YYYY');
                                }
                                
                                chresults[cf][columnName] = columnVal;
                            });
                            channelResults.push([channel, createdTimestamp, chresults]);
                            callback();
                        });
                    },
                    function(err){
                        channelResults.sort(function(a,b ) { return b[1] - a[1]; });
                        res.render('client',  { clientData: results, channels: channelResults, clientId: clientId })
                    });
            }
            else {
                res.render('client',  { clientData: results, clientId: clientId })
            }
        }
        else {
            res.render('e404',  { 'msg': 'This client does not exist' })
        }
    });
});

router.get('/channel', function(req, res) {
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientTable = hclient.table('clients');
    var channelTable = hclient.table('channels');
    var esclient = new elasticsearch.Client({
        host: 'localhost:9200'
        //log: 'trace'
    });
    var channelId = req.query.id;
    
    channelTable.row(channelId).get(function(error, data) {
        if (data) {
            channelData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in channelData)) {
                    channelData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();
                channelData[cf][columnName] = columnVal;
            });
            
            clientTable.row(channelData.info.client).get(function(error, data) {
                clientData = {};
                data.forEach(function(d) {
                    columnData = d.column.toString().split(':');
                    cf = columnData[0];
                    if (!(cf in clientData)) {
                        clientData[cf] = {};
                    } 
                    
                    columnName = columnData[1];
                    columnVal = d.$.toString();  
                    clientData[cf][columnName] = columnVal;
                });
                
                var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name};
                
                esclient.search({
                    index: 'c6_channel',
                    type: 'data',
                    body: {
                        "size": 0,
                        "query": {
                            "term": {
                                "channelId.raw": channelId
                            }
                        },
                        "aggs": {
                            "data_over_time" : {
                                "date_histogram" : {
                                    "field" : "time",
                                    "interval" : "day"
                                }
                            },
                            "stream_data" : {
                                "filter": {
                                    "term": {
                                        "ingestion": "stream"
                                    }
                                },
                                "aggs": {
                                    "stream_data_over_time": {
                                        "date_histogram" : {
                                            "field" : "time",
                                            "interval" : "day"
                                        }
                                    }
                                }
                            },
                            "by_ingestion_method" : {
                                "terms" : {
                                    "field" : "ingestion"
                                }
                            },
                            "by_action" : {
                                "terms" : {
                                    "field" : "action"
                                }
                            },
                            "distinct_users" : {
                                "cardinality" : {
                                    "field" : "userId"
                                }
                            },
                            "distinct_items" : {
                                "cardinality" : {
                                    "field" : "itemId"
                                }
                            }
                        }
                    }
                }).then(function (resp) {
                    console.log(resp)
                    var hits = resp.hits.total;
                    
                    var hitsByIngestionMethod = []
                    resp.aggregations.by_ingestion_method.buckets.forEach(function(bucket) {
                        hitsByIngestionMethod.push([bucket.key, parseInt(bucket.doc_count)])
                    })
                    
                    var hitsByAction = []
                    resp.aggregations.by_action.buckets.forEach(function(bucket) {
                        hitsByAction.push([bucket.key, parseInt(bucket.doc_count)])
                    })
                    
                    var chartData = [];
                    resp.aggregations.data_over_time.buckets.forEach(function(bucket) {
                       chartData.push([bucket.key_as_string.substring(0, 10), parseInt(bucket.doc_count), parseInt(bucket.doc_count) + 500]); 
                    });
                    
                    var streamChartData = [];
                    resp.aggregations.stream_data.stream_data_over_time.buckets.forEach(function(bucket) {
                       streamChartData.push([bucket.key_as_string.substring(0, 10), parseInt(bucket.doc_count), parseInt(bucket.doc_count) + 500]); 
                    });
                    
                    var numUsers = resp.aggregations.distinct_users.value
                    var numItems = resp.aggregations.distinct_items.value
                    
                    res.render('channel',  { clientData: clientData, channelData: channelData, channelId: channelId, selectedClient : selectedClient, 'moment': moment, 'chartData': chartData, 'streamChartData': streamChartData, 'hits': hits, 'hitsByIngestionMethod': hitsByIngestionMethod, 'hitsByAction': hitsByAction, numUsers: numUsers, numItems: numItems, 'jview': 'dashboard' }) 
                }, function (err) {
                    console.trace(err.message);
                });
            })
        }
        else {
            res.render('e404',  { 'msg': 'This channel does not exist' })
        }
    });
});

router.get('/getDataIngestionChartData', function(req, res) {
    var esclient = new elasticsearch.Client({
        host: 'localhost:9200'
        //log: 'trace'
    });
    var channelId = req.query.channel;
    var ingestionType = req.query.type;
    
    var esQuery = {
        index: 'c6_channel',
        type: 'data',
        body: {
            "size": 0,
            "query": {
                "filtered": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": { "channelId.raw": channelId } }
                            ]
                        }
                    }
                }
            },
            "aggs": {
                "data_over_time" : {
                    "date_histogram" : {
                        "field" : "time",
                        "interval" : "day"
                    }
                }
            }
        }
    }
     
    if(ingestionType == 'bulk' || ingestionType == 'stream') {
        esQuery['body']['query']['filtered']['filter']['bool']['must'].push({"term": { "ingestion": ingestionType } })
    }
     
    esclient.search(esQuery).then(function (resp) {
        var chartData = [];
        resp.aggregations.data_over_time.buckets.forEach(function(bucket) {
           chartData.push([bucket.key_as_string.substring(0, 10), parseInt(bucket.doc_count)]); 
        });
        res.json(chartData);
    }, function (err) {
        console.trace(err.message);
    });
});

router.get('/dataBrowser', function(req, res) {
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientTable = hclient.table('clients');
    var channelTable = hclient.table('channels');
    var esclient = new elasticsearch.Client({
        host: 'localhost:9200'
        //log: 'trace'
    });
    var channelId = req.query.id;
    
    channelTable.row(channelId).get(function(error, data) {
        if (data) {
            channelData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in channelData)) {
                    channelData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();
                channelData[cf][columnName] = columnVal;
            });
            
            clientTable.row(channelData.info.client).get(function(error, data) {
                clientData = {};
                data.forEach(function(d) {
                    columnData = d.column.toString().split(':');
                    cf = columnData[0];
                    if (!(cf in clientData)) {
                        clientData[cf] = {};
                    } 
                    
                    columnName = columnData[1];
                    columnVal = d.$.toString();  
                    clientData[cf][columnName] = columnVal;
                });
                
                var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name};
                
                esclient.search({
                    index: 'c6_channel',
                    type: 'data',
                    body: {
                        size: 100,
                        query: {
                            term: {
                                "channelId.raw": channelId
                            }
                        }
                    }
                }).then(function (resp) {
                    var hits = resp.hits.total;
                    res.render('dataBrowser',  { clientData: clientData, channelData: channelData, channelId: channelId, selectedClient : selectedClient, 'moment': moment, 'data': resp.hits.hits, 'hits': hits})
                }, function (err) {
                    console.trace(err.message);
                });
            })
        }
        else {
            res.render('e404',  { 'msg': 'This channel does not exist' })
        }
    });
});

router.get('/testAPI', function(req, res) {
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientTable = hclient.table('clients');
    var channelTable = hclient.table('channels');
    var algoTable = hclient.table('algos');
    var channelId = req.query.id;
    var rtype = req.query.type;
    if (!rtype) {
        rtype = 'user';
    }
    
    var algoMasterList = {"cooccurrence": "ICF: Co-occurrence",
                          "jaccard": "ICF: Jaccard",
                          "cosine": "ICF: Cosine",
                          "loglikelihood" : "ICF: LogLikelihood",
                          "pearson" : "ICF: Pearson Correlation",
                          "als" : "MF: ALS",
                          "alsimplicit" : "MF: Implicit ALS",
                          "contentbased" : "Content Based Similarity" 
                        }
    
    channelTable.row(channelId).get(function(error, data) {
        if (data) {
            channelData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':'); 
                cf = columnData[0];
                if (!(cf in channelData)) {
                    channelData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();
                channelData[cf][columnName] = columnVal;
            });
            
            clientTable.row(channelData.info.client).get(function(error, data) {
                clientData = {};
                data.forEach(function(d) {
                    columnData = d.column.toString().split(':');
                    cf = columnData[0];
                    if (!(cf in clientData)) {
                        clientData[cf] = {};
                    } 
                    
                    columnName = columnData[1];
                    columnVal = d.$.toString();  
                    clientData[cf][columnName] = columnVal;
                });
                
                var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name};
                
                algoTable.row(channelId+':*').get(function(error, data) {
                    algoResults = {};
                    data.forEach(function(d) {
                        algoId = d.key.toString();
                        algoIdParts= algoId.split(':')
                        algoId = algoIdParts[1].toLowerCase()
                        if (!(algoId in algoResults)) {
                            algoResults[algoId] = {};
                        }
                        columnData = d.column.toString().split(':');
                        if (columnData[0] == 'info') {
                            columnName = columnData[1];
                            columnVal = d.$.toString();
                            algoResults[algoId][columnName] = columnVal;
                        }
                    })
                    res.render('testAPI',  { clientData: clientData, channelData: channelData, channelId: channelId, selectedClient : selectedClient, rtype: rtype, algoMasterList: algoMasterList, defaultNum: 20, algoResults: algoResults, 'jview': 'testAPI' }) 
                }) 
            })
        }
        else {
            res.render('e404',  { 'msg': 'This channel does not exist' })
        }
    });
});


router.get('/bulkUploads', function(req, res) {
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientTable = hclient.table('clients');
    var channelTable = hclient.table('channels');
    var bulkUploadTable = hclient.table('bulkuploads');
    var channelId = req.query.id;
    var esclient = new elasticsearch.Client({
        host: 'localhost:9200'
        //log: 'trace'
    });
    
    channelTable.row(channelId).get(function(error, data) {
        if (data) {
            channelData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in channelData)) {
                    channelData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();
                channelData[cf][columnName] = columnVal;
            });
            
            clientTable.row(channelData.info.client).get(function(error, data) {
                clientData = {};
                data.forEach(function(d) {
                    columnData = d.column.toString().split(':');
                    cf = columnData[0];
                    if (!(cf in clientData)) {
                        clientData[cf] = {};
                    } 
                    
                    columnName = columnData[1];
                    columnVal = d.$.toString();  
                    clientData[cf][columnName] = columnVal;
                });
                
                var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name};
                
                bulkUploadTable.row(channelId+':*').get(function(error, data) {
                    
                    if (data) {
                        bulkUploadResults = {};
                        data.forEach(function(d) {
                            rowKeyParts = d.key.toString().split(':');
                            
                            buTime = rowKeyParts[1]
                            buId = rowKeyParts[2]
                            
                            if (!(buId in bulkUploadResults)) {
                                bulkUploadResults[buId] = {'uploadTime': buTime};
                            }
                            columnData = d.column.toString().split(':');
                            
                            columnName = columnData[1];
                            columnVal = d.$.toString();
                            bulkUploadResults[buId][columnName] = columnVal;
                        });
                        
                        var uploadedFiles = Object.keys(bulkUploadResults);
                        var bulkUploadData = {}
                        
                        async.each(uploadedFiles, function(uploadedFile, callback){
                            esclient.search({
                                index: 'c6_channel',
                                type: 'data',
                                body: {
                                    size: 0,
                                    query: {
                                        term: {
                                            "bulkfile.raw": uploadedFile
                                        }
                                    },
                                    aggs: {
                                        data_over_time : {
                                            date_histogram : {
                                                field : "time",
                                                interval : "day"
                                            }
                                        }
                                    }
                                }
                            }).then(function (resp) {
                                var hits = resp.hits.total;
                                var chartData = [];
                                resp.aggregations.data_over_time.buckets.forEach(function(bucket) {
                                   chartData.push([bucket.key_as_string.substring(0, 10), parseInt(bucket.doc_count)]); 
                                });
                                
                                bulkUploadData[uploadedFile] = {'hits': hits, 'chartData': chartData}
                                
                                callback();
                                
                            }, function (err) {
                                console.trace(err.message);
                                callback();
                            });
                        },
                        function(err){
                            res.render('bulkUploads',  { clientData: clientData, channelData: channelData, channelId: channelId, selectedClient : selectedClient, 'jview': 'bulkuploads', 'moment': moment, bulkUploadResults: bulkUploadResults, 'bulkUploadData': bulkUploadData }) 
                            
                        });
                    }
                    else {
                        res.render('bulkUploads',  { clientData: clientData, channelData: channelData, channelId: channelId, selectedClient : selectedClient, 'jview': 'bulkuploads', 'moment': moment, 'chartData': [] })
                    }
                })
            })
        }
        else {
            res.render('e404',  { 'msg': 'This channel does not exist' })
        }
    });
});

router.get('/addClient', function(req, res) {
    res.render('addClient', {});
});

router.get('/addChannel', function(req, res) {
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientTable = hclient.table('clients');
    var clientId = req.query.client;
    
    clientTable.row(clientId).get(function(error, data) {
        results = {};
        data.forEach(function(d) {
            columnData = d.column.toString().split(':');
            cf = columnData[0];
            if (!(cf in results)) {
                results[cf] = {};
            }
            
            columnName = columnData[1];
            columnVal = d.$.toString();
            results[cf][columnName] = columnVal;
        });
        res.render('addChannel',  { clientData: results, clientId: clientId })
    });
});

router.get('/bulkUpload', function(req, res) {
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientTable = hclient.table('clients');
    var channelTable = hclient.table('channels');
    var channelId = req.query.channel;
    
    channelTable.row(channelId).get(function(error, data) {
        if (data) {
            channelData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in channelData)) {
                    channelData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();
                channelData[cf][columnName] = columnVal;
            });
            
            clientTable.row(channelData.info.client).get(function(error, data) {
                clientData = {};
                data.forEach(function(d) {
                    columnData = d.column.toString().split(':');
                    cf = columnData[0];
                    if (!(cf in clientData)) {
                        clientData[cf] = {};
                    }
                    
                    columnName = columnData[1];
                    columnVal = d.$.toString();  
                    clientData[cf][columnName] = columnVal;
                });
                var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name}; 
                res.render('bulkUpload',  { clientData: clientData, channelData: channelData, channelId: channelId, selectedClient: selectedClient, 'jview': 'bulkuploads' })
            })
        }
        else {
            res.render('e404',  { 'msg': 'This channel does not exist' })
        }
    });
});

router.post('/saveBulkUpload', function(req, res) {
    var channelId = req.body.channelId;
    var clientId = req.body.clientId;
    var desc = req.body.bulkupload_desc;
    
    var fs = require('fs');
    var fileName = '';
    
    var hdfs = WebHDFS.createClient();
    var redisClient = redis.createClient();
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var bulkUploadTable = hclient.table('bulkuploads');
    
    var currentTime = (new Date).getTime();
    
    if (req.files) {
        bulkUpload = req.files.bulk_upload_data_file;
        originalName = bulkUpload.originalname;
        fileName = bulkUpload.name;
        filePath = bulkUpload.path;
        
        hdfsPath = '/r1/'+clientId+'/'+channelId+'/bulk/'+fileName
        
        var localFileStream = fs.createReadStream(filePath);
        var remoteFileStream = hdfs.createWriteStream(hdfsPath);
        
        localFileStream.pipe(remoteFileStream);
        
        fileNameParts = fileName.split('.');
        uploadId = fileNameParts[0];
        
        remoteFileStream.on('finish', function onFinish () {
            var uploadRowKey = channelId+':'+currentTime.toString()+':'+uploadId;
            bulkUploadTable.row(uploadRowKey).put(['info:name', 'info:desc', 'info:oname', 'info:path', 'info:processed'], [fileName, desc, originalName, hdfsPath, 'no'], function(error, success) {
                redisClient.publish("JobsToRun", "BULKUPLOAD#"+uploadRowKey);
                res.redirect('/bulkUploads?id='+channelId);
            });
        });
    }
})

router.post('/saveClient', function(req, res) {
    var clientName = req.body.client_name; 
    var clientDesc = req.body.client_desc;
    var currentTime = (new Date).getTime();
    var clientId = "CL-"+currentTime.toString(16).toUpperCase();
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientInfoTable = hclient.table('clients');
    
    clientInfoTable.row(clientId).put(['info:name', 'info:desc', 'info:created'], [clientName, clientDesc, currentTime.toString()], function(error, success) {
        if (success) {
            res.redirect('/');    
        }
    });
});

router.get('/channelSettings', function(req, res) {
    var channel = req.query.id;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 })
    var channelTable = hclient.table('channels');
    var clientTable = hclient.table('clients');
    var algoTable = hclient.table('algos');
    
    var algoMasterList = {"cooccurrence": "ICF: Co-occurrence",
                          "jaccard": "ICF: Jaccard",
                          "cosine": "ICF: Cosine",
                          "loglikelihood" : "ICF: LogLikelihood",
                          "pearson" : "ICF: Pearson Correlation",
                          "als" : "MF: ALS",
                          "alsimplicit" : "MF: Implicit ALS",
                          "contentbased" : "Content Based"
                        }
    
    var algoListCategorised = {"Item-based Collaborative Filtering": ["cooccurrence", "jaccard", "cosine", "loglikelihood", "pearson"], "Matrix Factorization" : ["als", "alsimplicit"], "Content Based": ["contentbased"]}
    
    channelTable.row(channel).get(function(error, data) {
        channelData = {};
        data.forEach(function(d) {
            columnData = d.column.toString().split(':');
            cf = columnData[0];
            if (!(cf in channelData)) {
                channelData[cf] = {};
            }
            
            columnName = columnData[1];
            columnVal = d.$.toString();  
            channelData[cf][columnName] = columnVal;
        });
        
        clientTable.row(channelData.info.client).get(function(error, data) {
            clientData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in clientData)) {
                    clientData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();  
                clientData[cf][columnName] = columnVal;
            });
            
            var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name};
            
            algoTable.row(channel+':*').get(function(error, data) {
                algoResults = {};
                data.forEach(function(d) {
                    algoId = d.key.toString();
                    if (!(algoId in algoResults)) {
                        algoResults[algoId] = {};
                    }
                    columnData = d.column.toString().split(':');
                    console.log(columnData)
                    if (columnData[0] == 'info') {
                        columnName = columnData[1];
                        columnVal = d.$.toString();
                        algoResults[algoId][columnName] = columnVal;
                    }
                })
                //console.log(algoResults) 
                res.render('channelSettings',  { clientData: clientData, channelData: channelData, channelId: channel, selectedClient: selectedClient, algoMasterList: algoMasterList, algoListCategorised: algoListCategorised, algoResults: algoResults, 'jview': 'algos' })  
            })
        });
    });
})

router.get('/contentBasedAttributes', function(req, res) {
    var channel = req.query.id;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 })
    var channelTable = hclient.table('channels');
    var clientTable = hclient.table('clients');
    var attributeTable = hclient.table('contentBasedAttributes');
    
    channelTable.row(channel).get(function(error, data) {
        channelData = {};
        data.forEach(function(d) {
            columnData = d.column.toString().split(':');
            cf = columnData[0];
            if (!(cf in channelData)) {
                channelData[cf] = {};
            }
            
            columnName = columnData[1];
            columnVal = d.$.toString();  
            channelData[cf][columnName] = columnVal;
        });
        
        clientTable.row(channelData.info.client).get(function(error, data) {
            clientData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in clientData)) {
                    clientData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();  
                clientData[cf][columnName] = columnVal;
            });
            
            var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name};
            
            attributeTable.row(channel+':*').get(function(error, data) {
                algoResults = {}
            
                attributeResults = {};
                data.forEach(function(d) {
                    rowKey = d.key.toString();
                    rowKeyParts = rowKey.split(':')
                    attributeKey = rowKeyParts[1]
                    if (attributeKey) {
                        if (!(attributeKey in attributeResults)) {
                            attributeResults[attributeKey] = {};
                        }
                        columnData = d.column.toString().split(':');
                        if (columnData[0] == 'info') {
                            columnName = columnData[1];
                            columnVal = d.$.toString();
                            attributeResults[attributeKey][columnName] = columnVal;
                        }    
                    }
                })
                console.log(attributeResults)
                res.render('contentBasedAttributes',  { clientData: clientData, channelData: channelData, channelId: channel, selectedClient: selectedClient, attributeResults: attributeResults, 'jview': 'contentAttributes' })  
            })
        });
    });
})

router.get('/jobs', function(req, res) {
    var channel = req.query.id;
    var statusCheck = req.query.statusCheck;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 })
    var channelTable = hclient.table('channels');
    var clientTable = hclient.table('clients');
    var algoTable = hclient.table('algos');
    
    var algoMasterList = {"cooccurrence": "ICF: Co-occurrence",
                          "jaccard": "ICF: Jaccard",
                          "cosine": "ICF: Cosine",
                          "loglikelihood" : "ICF: LogLikelihood",
                          "pearson" : "ICF: Pearson Correlation",
                          "als" : "MF: ALS",
                          "alsimplicit" : "MF: Implicit ALS",
                          "contentbased" : "Content Based Similarity" 
                        }
    
    channelTable.row(channel).get(function(error, data) {
        channelData = {};
        data.forEach(function(d) {
            columnData = d.column.toString().split(':');
            cf = columnData[0];
            if (!(cf in channelData)) {
                channelData[cf] = {};
            }
            
            columnName = columnData[1];
            columnVal = d.$.toString();  
            channelData[cf][columnName] = columnVal;
        });
        
        clientTable.row(channelData.info.client).get(function(error, data) {
            clientData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in clientData)) {
                    clientData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();  
                clientData[cf][columnName] = columnVal;
            });
            
            var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name};
            
            algoTable.row(channel+':*').get(function(error, data) {
                algoResults = {};
                data.forEach(function(d) {
                    algoId = d.key.toString();
                    if (!(algoId in algoResults)) {
                        algoResults[algoId] = {};
                    }
                    columnData = d.column.toString().split(':');
                    if (columnData[0] == 'info') {
                        columnName = columnData[1];
                        columnVal = d.$.toString();
                        algoResults[algoId][columnName] = columnVal;
                    }
                })
                
                if (statusCheck == '1') {
                    res.app.render('jobs',  { clientData: clientData, channelData: channelData, channelId: channel, selectedClient: selectedClient, algoMasterList: algoMasterList, algoResults: algoResults, statusCheck: true, 'jview': 'jobs'}, function(err, html){
                        res.json({'response': html})
                    });
                }
                else {
                    res.render('jobs',  { clientData: clientData, channelData: channelData, channelId: channel, selectedClient: selectedClient, algoMasterList: algoMasterList, algoResults: algoResults, statusCheck: false, 'jview': 'jobs' })
                }
            })
        });
    });
})

router.get('/jobHistory', function(req, res) {
    var algoId = req.query.id;
    var channel = req.query.channel;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 })
    var channelTable = hclient.table('channels');
    var clientTable = hclient.table('clients');
    var algoTable = hclient.table('algos');
    
    channelTable.row(channel).get(function(error, data) {
        channelData = {};
        data.forEach(function(d) {
            columnData = d.column.toString().split(':');
            cf = columnData[0];
            if (!(cf in channelData)) {
                channelData[cf] = {};
            }
            
            columnName = columnData[1];
            columnVal = d.$.toString();  
            channelData[cf][columnName] = columnVal;
        });
        
        clientTable.row(channelData.info.client).get(function(error, data) {
            clientData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in clientData)) {
                    clientData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();  
                clientData[cf][columnName] = columnVal;
            });
            
            var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name};
            
            algoTable.row(algoId).get(function(error, data) {
                algoResults = {};
                algoResults['jobHistory'] = []
                data.forEach(function(d) {
                    columnData = d.column.toString().split(':');
                    if (columnData[0] == 'info') {
                        columnName = columnData[1];
                        columnVal = d.$.toString();
                        algoResults[columnName] = columnVal;
                    }
                    else if (columnData[0] == 'jobhistory') {
                        columnName = columnData[1];
                        columnVal = d.$.toString();
                        algoResults['jobHistory'].push([parseInt(columnName), JSON.parse(columnVal)]);
                    }
                })
                 
                algoResults['jobHistory'].sort(function(a, b) { return b[0]-a[0] })
                
                var chartData = [];
                algoResults['jobHistory'].forEach(function(hist) {
                    barcolor = '#DB4437'
                    if (hist[1].outcome == 'success') {
                        barcolor = '#03B567'
                    }
                    chartData.push([moment(hist[0]).format('YYYY-MM-DD HH:mm:ss'), parseInt((parseInt(hist[1].endTime) - parseInt(hist[1].startTime))/1000), barcolor]); 
                });
                console.log(chartData)
                res.render('jobHistory',  { clientData: clientData, channelData: channelData, channelId: channel, selectedClient: selectedClient, algoResults: algoResults, statusCheck: false, moment: moment, chartData: chartData, 'jview': 'jobs' })
            })
        });
    });
})

router.get('/runJob', function(req, res) {
    var algoId = req.query.id; 
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var algoTable = hclient.table('algos');
    
    var redisClient = redis.createClient();

    algoTable.row(algoId).put('info:status', 'queued', function(error, success) {
        if (success) {
            redisClient.publish("JobsToRun", "PROCESSALGO#"+algoId);
            res.json([])    
        }
    });
});

router.get('/editChannelInfo', function(req, res) {
    var channel = req.query.id;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 })
    var channelTable = hclient.table('channels');
    var clientTable = hclient.table('clients');
    
    channelTable.row(channel).get(function(error, data) {
        channelData = {};
        data.forEach(function(d) {
            columnData = d.column.toString().split(':');
            cf = columnData[0];
            if (!(cf in channelData)) {
                channelData[cf] = {};
            }
            
            columnName = columnData[1];
            columnVal = d.$.toString();  
            channelData[cf][columnName] = columnVal;
        });
        
        clientTable.row(channelData.info.client).get(function(error, data) {
            clientData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in clientData)) {
                    clientData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();  
                clientData[cf][columnName] = columnVal;
            });
            
            var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name};
            
            res.render('editChannelInfo',  { clientData: clientData, channelData: channelData, channelId: channel, selectedClient: selectedClient, 'jview': 'editChannel' })
        });
    });
})

router.get('/viewData', function(req, res) {
    var channel = req.query.id;
    var item = parseFloat(req.query.item);
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 })
    var channelTable = hclient.table('channels');
    var clientTable = hclient.table('clients');
    var attributeTable = hclient.table('contentBasedAttributes');
    
    var esclient = new elasticsearch.Client({
        host: 'localhost:9200'
    });
    
    channelTable.row(channel).get(function(error, data) {
        channelData = {};
        data.forEach(function(d) {
            columnData = d.column.toString().split(':');
            cf = columnData[0];
            if (!(cf in channelData)) {
                channelData[cf] = {};
            }
            
            columnName = columnData[1];
            columnVal = d.$.toString();  
            channelData[cf][columnName] = columnVal;
        });
        
        clientTable.row(channelData.info.client).get(function(error, data) {
            clientData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in clientData)) {
                    clientData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();  
                clientData[cf][columnName] = columnVal;
            });
            
            var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name};
            
            if (item) {
                attributeTable.row(channel+':*').get(function(error, data) {
                    attributeResults = {};
                    data.forEach(function(d) {
                        rowKey = d.key.toString();
                        rowKeyParts = rowKey.split(':')
                        attributeKey = rowKeyParts[1]
                        if (attributeKey) {
                            if (!(attributeKey in attributeResults)) {
                                attributeResults[attributeKey] = {};
                            }
                            columnData = d.column.toString().split(':');
                            if (columnData[0] == 'info') {
                                columnName = columnData[1];
                                columnVal = d.$.toString();
                                attributeResults[attributeKey][columnName] = columnVal;
                            }    
                        }
                    })
                    
                    esclient.search({
                        index: 'content_based_data2',
                        type: 'data',
                        body: {
                            size: 100,
                            query: {
                                bool: {
                                    must: [
                                        {
                                            term: {
                                                "channelId.raw": channel
                                            }
                                        },
                                        {
                                            term: {
                                                "itemId": item
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }).then(function (resp) {
                        
                        contentData = {}
                        if (resp['hits']['total'] > 0) {
                            contentData = resp['hits']['hits'][0]['_source']
                        }
                        
                        var esQuery = {
                            index: 'c6_channel',
                            type: 'data',
                            body: {
                                "size": 0,
                                "query": {
                                    "filtered": {
                                        "filter": {
                                            "bool": {
                                                "must": [
                                                    {"term": { "channelId.raw": channel } },
                                                    {"term": { "itemId": item } },
                                                ]
                                            }
                                        }
                                    }
                                },
                                "aggs": {
                                    "data_over_time" : {
                                        "date_histogram" : {
                                            "field" : "time",
                                            "interval" : "day"
                                        }
                                    }
                                }
                            }
                        }
                          
                        esclient.search(esQuery).then(function (resp) {
                            var chartData = [];
                            resp.aggregations.data_over_time.buckets.forEach(function(bucket) {
                               chartData.push([bucket.key_as_string.substring(0, 10), parseInt(bucket.doc_count)]); 
                            });
                    
                            res.render('viewData',  { clientData: clientData, channelData: channelData, channelId: channel, selectedClient: selectedClient, 'jview': 'viewData', 'contentData': contentData, 'itemId': item, 'attributeResults': attributeResults, chartData: chartData }) 
                            
                        }, function (err) {
                            console.trace(err.message);
                        });
                    }, function (err) {
                        console.trace(err.message);
                    });    
                })
            }
            else {
                res.render('viewData',  { clientData: clientData, channelData: channelData, channelId: channel, selectedClient: selectedClient, 'jview': 'viewData', 'contentData': {}, 'itemId': item, 'attributeResults': {}, 'chartData': {} }) 
            }
        });
    });
})


router.get('/attributeCoverage', function(req, res) {
    var channel = req.query.id;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 })
    var channelTable = hclient.table('channels');
    var clientTable = hclient.table('clients');
    var attributeTable = hclient.table('contentBasedAttributes');
    
    var esclient = new elasticsearch.Client({
        host: 'localhost:9200'
    });
    
    channelTable.row(channel).get(function(error, data) {
        channelData = {};
        data.forEach(function(d) {
            columnData = d.column.toString().split(':');
            cf = columnData[0];
            if (!(cf in channelData)) {
                channelData[cf] = {};
            }
            
            columnName = columnData[1];
            columnVal = d.$.toString();  
            channelData[cf][columnName] = columnVal;
        });
        
        clientTable.row(channelData.info.client).get(function(error, data) {
            clientData = {};
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                cf = columnData[0];
                if (!(cf in clientData)) {
                    clientData[cf] = {};
                }
                
                columnName = columnData[1];
                columnVal = d.$.toString();  
                clientData[cf][columnName] = columnVal;
            });
            
            var selectedClient = {'id' : channelData.info.client, 'name' : clientData.info.name};
            
            attributeTable.row(channel+':*').get(function(error, data) {
                attributeResults = {};
                data.forEach(function(d) {
                    rowKey = d.key.toString();
                    rowKeyParts = rowKey.split(':')
                    attributeKey = rowKeyParts[1]
                    if (attributeKey) {
                        if (!(attributeKey in attributeResults)) {
                            attributeResults[attributeKey] = {};
                        }
                        columnData = d.column.toString().split(':');
                        if (columnData[0] == 'info') {
                            columnName = columnData[1];
                            columnVal = d.$.toString();
                            attributeResults[attributeKey][columnName] = columnVal;
                        }    
                    }
                })
                
                aggs = {}
                for (aKey in attributeResults) {
                    if (attributeResults[aKey]['type'] == 'numeric') {
                        attrAggs = {"filter": {"range": {}}}
                        attrAggs["filter"]["range"][aKey] = {"gt": 0}
                    }
                    else {
                        attrAggs = {"filter": {"exists": {"field": aKey}}}
                    }
                    aggs[aKey] = attrAggs
                }
                
                esclient.search({
                    index: 'content_based_data2',
                    type: 'data',
                    body: {
                        size: 0,
                        query: {
                            term: {
                                "channelId.raw": channel
                            }
                        },
                        aggs: aggs
                    }
                }).then(function (resp) {
                    
                    totalDocs = resp['hits']['total']
                    
                    res.render('attributeCoverage',  { clientData: clientData, channelData: channelData, channelId: channel, selectedClient: selectedClient, 'jview': 'viewData', 'attributeResults': attributeResults, totalDocs: totalDocs, 'coverage': resp['aggregations'] })   
                    
                }, function (err) {
                    console.trace(err.message);
                });    
            })
        });
    });
})

router.post('/saveChannel', function(req, res) {
    var channelName = req.body.channel_name; 
    var channelDesc = req.body.channel_desc;
    var clientId = req.body.clientId;
    var clientNew = req.body.clientNew;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientInfoTable = hclient.table('clients');
    var channelInfoTable = hclient.table('channels');
    
    var currentTime = (new Date).getTime();
    var channelId = "CH-"+currentTime.toString(16).toUpperCase();
    
    
    clientDataPutKeys = ['channels:'+channelId];
    clientDataPutValues = ['1'];
    
    /**
     * Create new client
     */ 
    if (!clientId) {
        clientId = "CL-"+currentTime.toString(16).toUpperCase();
        clientDataPutKeys.push('info:name');
        clientDataPutKeys.push('info:created');
        clientDataPutValues.push(clientNew);
        clientDataPutValues.push(currentTime.toString());
    }
    
    clientInfoTable.row(clientId).put(clientDataPutKeys, clientDataPutValues, function(error, success) {
        if (success) {
            channelInfoTable.row(channelId).put(['info:name', 'info:desc', 'info:created', 'info:client'], [channelName, channelDesc, currentTime.toString(), clientId], function(error, success) {
                if (success) {
                    var hdfs = WebHDFS.createClient();    
                    hdfs.exists('/r1/'+clientId+'/'+channelId, function(exists) {
                        if(!exists) {
                            hdfs.mkdir('/r1/'+clientId+'/'+channelId+'/bulk', '0777', function(error) {});
                            hdfs.mkdir('/r1/'+clientId+'/'+channelId+'/stream', '0777', function(error) {}); 
                        }
                        res.redirect('/channel?id='+channelId);
                    });
                }
            });
        }
    });
});

router.post('/saveChannelSettings', function(req, res) {
    var channelName = req.body.channel_name; 
    var channelDesc = req.body.channel_desc;
    var clientName = req.body.client_name; 
    var clientId = req.body.clientId;
    var channelId = req.body.channelId;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var clientInfoTable = hclient.table('clients');
    var channelInfoTable = hclient.table('channels');
    
    clientInfoTable.row(clientId).put('info:name', clientName, function(error, success) {
        if (success) {
            channelInfoTable.row(channelId).put(['info:name', 'info:desc'], [channelName, channelDesc], function(error, success) {
                if (success) {
                    res.redirect('/channel?id='+channelId);
                }
            });
        }
    });
});

router.get('/deleteAlgo', function(req, res) {
    var algoId = req.query.id;
    var algoIdParts = algoId.split(':')
    var channelId = algoIdParts[0];
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var algoTable = hclient.table('algos');
    
    algoTable.row(algoId).delete(function(error, success) {
        if (success) {
            res.redirect('/channelSettings?id='+channelId);
        }
    });
});

router.get('/deleteAttribute', function(req, res) {
    var attributeId = req.query.id;
    var channelId = req.query.channel;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var attributeTable = hclient.table('contentBasedAttributes');
    
    attributeTable.row(channelId+":"+attributeId).delete(function(error, success) {
        if (success) {
            res.redirect('/contentBasedAttributes?id='+channelId);
        }
    });
});

router.post('/addAlgo', function(req, res) {
    var algoName = req.body.algo_name; 
    var baseAlgo = req.body.base_algo;
    var actions = req.body.actions;
    var missingValues = req.body.missing_values;
    var channelId = req.body.channelId;
    
    var currentTime = (new Date).getTime();
    var algoId = "AL-"+currentTime.toString(16).toUpperCase();
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var algoTable = hclient.table('algos');
    
    var rowKey = channelId+':'+algoId
    
    actionList = []
    
    actions = trim(actions)
    if (actions) {
        actionList = actions.split(',')
        for(var j=0;j<actionList.length;j++) {
            actionList[j] = trim(actionList[j])
        }
    }
    
    algoTable.row(rowKey).put(['info:name', 'info:base', 'info:actions', 'info:missingValues'], [algoName, baseAlgo, JSON.stringify(actionList), missingValues], function(error, success) {
        if (success) {
            res.redirect('/channelSettings?id='+channelId);
        }
    });
});

router.post('/addContentBasedAttribute', function(req, res) {
    var attributeName = req.body.attribute_name;
    var attributeKey = req.body.attribute_key;
    var attributeType = req.body.attribute_type; 
    
    var channelId = req.body.channelId;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var attributeTable = hclient.table('contentBasedAttributes');
    
    var rowKey = channelId+':'+attributeKey
    
    attributeTable.row(rowKey).put(['info:name', 'info:key', 'info:type'], [attributeName, attributeKey, attributeType], function(error, success) {
        if (success) {
            res.redirect('/contentBasedAttributes?id='+channelId);
        }
    });
});

router.get('/recommendations', function(req, res) {

    var userId = parseInt(req.query.user);
    var num = parseInt(req.query.num);
    var channel = req.query.channel;
    var algo = req.query.algo;
    
    if (num <= 0) {
        num = 20;
    }
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var userTable = hclient.table('c1.users');
    var similarityTable = hclient.table('s1.similarity');
    var recommendationTable = hclient.table('s1.recommendations');
    var algoTable = hclient.table('algos');
    
    algoTable.row(channel.toUpperCase()+":"+algo.toUpperCase()).get('info', function(error, data) {
        if (data) {
            var version = '';
            var baseAlgo = '';
            
            data.forEach(function(d) {
                if(d.column.toString() == 'info:sversion') {
                    version = d.$.toString();
                }
                else if(d.column.toString() == 'info:base') {
                    baseAlgo = d.$.toString();
                }
            })
            
            if (baseAlgo == 'als' || baseAlgo == 'alsimplicit') {
                recommendationTable.row(channel.toLowerCase()+':'+algo+':'+userId+':'+version).get(function(error, data) {
                    if (data) {
                        recommendedItemsArr = []
                        data.forEach(function(d) {
                            columnData = d.column.toString().split(':');
                            ritemId = parseInt(columnData[1]);
                            score = parseFloat(d.$.toString())
                            recommendedItemsArr.push([ritemId, score]);
                        });
                        recommendedItemsArr.sort(function(a, b) { return b[1] - a[1] }).slice(0, 20)
                        recommendedItemsArr = recommendedItemsArr.slice(0, num)
                        console.log(recommendedItemsArr)
                        response = []
                        recommendedItemsArr.forEach(function(recommendedItem) {
                            response.push({'item': recommendedItem[0], 'score': recommendedItem[1]})
                        })
                        res.json(response)
                    }
                    else {
                        res.json([])
                    }
                });
            }
            else {
                userTable.row(channel+":"+userId.toString()).get(function(error, data) {
                    if (data) {
                        userData = [];
                        userItems = {}
                        userRatings = {}
                        data.forEach(function(d) {
                            columnData = d.column.toString().split(':');
                            columnDataVal = d.$.toString()
                            userData.push([columnData[1], columnData[2], columnDataVal])
                            userItems[columnData[2]] = true
                            
                            columnDataValParts = columnDataVal.split(':')
                            if (columnDataValParts.length == 2 && columnDataValParts[0] == 'rating') {
                                userRatings[columnData[2]] = parseFloat(columnDataValParts[1])
                            }
                        });
                        userData.sort(function(a, b) { return b[0] - a[0]; });
                        userItems = Object.keys(userItems)
                        
                        recommendedItems = {}
                        recommendedItemsArr = []
                        
                        async.each(userItems, function(itemId, callback){
                            similarityTable.row(channel.toLowerCase()+':'+algo+':'+itemId+':'+version).get(function(error, data) {
                                data.forEach(function(d) {
                                    columnData = d.column.toString().split(':');
                                    score = parseInt(columnData[1]);
                                    ritemId = parseInt(columnData[2]);
                                    
                                    if (ritemId in recommendedItems) {
                                        if (itemId in userRatings) {
                                            recommendedItems[ritemId][0] += score * userRatings[itemId];
                                            recommendedItems[ritemId][1] += score;
                                        }
                                        else {
                                            recommendedItems[ritemId][0] += score
                                        }
                                    }
                                    else {
                                        recommendedItems[ritemId] = []
                                        if (itemId in userRatings) {
                                            recommendedItems[ritemId][0] = score * userRatings[itemId];
                                            recommendedItems[ritemId][1] = score;
                                        }
                                        else {
                                            recommendedItems[ritemId][0] = score;
                                            recommendedItems[ritemId][1] = 1;
                                        }
                                    }
                                });
                                callback();
                            });
                        },
                        function(err){
                            for (rItem in recommendedItems) {
                                recommendedItemsArr.push([parseInt(rItem), recommendedItems[rItem][0]/recommendedItems[rItem][1]])
                            }
                            recommendedItemsArr.sort(function(a, b) { return b[1] - a[1] }).slice(0, 20)
                            recommendedItemsArr = recommendedItemsArr.slice(0, num)
                        
                            response = []
                            recommendedItemsArr.forEach(function(recommendedItem) {
                                response.push({'item': recommendedItem[0], 'score': recommendedItem[1]})
                            })
                            res.json(response)
                        }); 
                    }
                    else {
                        res.json([])        
                    }
                })
            }
        }
        else {
            res.json([])
        }
    });
});

router.get('/userHistory', function(req, res) {

    var userId = parseInt(req.query.user);
    var channel = req.query.channel;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var userTable = hclient.table('c1.users');
    var similarityTable = hclient.table('s1.similarity');
    var channelTable = hclient.table('channels');
    
    userTable.row(channel+":"+userId.toString()).get(function(error, data) {
        if (data) {
            userData = [];
            userItems = {}
            data.forEach(function(d) {
                columnData = d.column.toString().split(':');
                userData.push([columnData[1], columnData[2], d.$.toString()])
            });
            
            userData.sort(function(a, b) { return b[0] - a[0] });
            
            response = []
            userData.forEach(function(userDataItem) {
                var action = userDataItem[2]
                var actionParts = action.split(':')
                if (actionParts.length == 2 && actionParts[0] == 'rating') {
                    response.push({'item': parseInt(userDataItem[1]), 'rating': parseFloat(actionParts[1]), 'time': moment(parseInt(userDataItem[0])*1000).format('YYYY-MM-DD HH:mm:ss')})
                }
                else {
                    response.push({'item': parseInt(userDataItem[1]), 'action': userDataItem[2], 'time': moment(parseInt(userDataItem[0])*1000).format('YYYY-MM-DD HH:mm:ss')})
                }
            })
            res.json(response)
        }
        else {
            res.json([])        
        }
    });
});


router.post('/getSimilarItems', function(req, res) {

    var itemId = parseInt(req.query.item);
    var channel = req.query.channel;
    var algos = req.query.algos.split(',');
    var algoList = {"cooccurrence": "CF: Co-occurrence", "jaccard": "CF: Jaccard", "cosine": "CF: Cosine"}
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var similarityTable = hclient.table('s1.similarity');
    var channelTable = hclient.table('channels');
    
    similarItems = {}
    
    channelTable.row(channel).get('info:sversion', function(error, data) {
        if (data) {
            var version = data[0].$.toString();
            async.each(algos, function(algo, callback){
                similarityTable.row(channel.toLowerCase()+':'+algo+':'+itemId+':'+version).get(function(error, data) {
                    algoSimilarItems = []
                    if (data) {
                        data.forEach(function(d) {
                            columnData = d.column.toString().split(':');
                            score = parseInt(columnData[1]);
                            ritemId = parseInt(columnData[2]);            
                            algoSimilarItems.push([ritemId, score]);
                        });
                        
                        algoSimilarItems.sort(function(a,b ) { return b[1] - a[1]; })
                        algoSimilarItems = algoSimilarItems.slice(0, 20)
                    }
                    similarItems[algo] = algoSimilarItems
                    callback()
                });
            },
            function(err){
                res.app.render('similar', {similarItems: similarItems, algos: algos, algoList: algoList}, function(err, html){
                    res.json({'response': html})
                });
            });
        }
        else {
            res.json({"response": ""})
        }
    })
});

router.get('/similarItems', function(req, res) {
    var itemId = parseInt(req.query.id); 
    var num = parseInt(req.query.num);
    var algo = req.query.algo;
    var channel = req.query.channel;
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    var similarityTable = hclient.table('s1.similarity');
    var algoTable = hclient.table('algos');
    
    if (num <= 0) {
        num = 10;
    }

    similarItems = []
    
    algoTable.row(channel.toUpperCase()+":"+algo.toUpperCase()).get('info:sversion', function(error, data) {
        console.log(data)
        if (data) {
            var version = data[0].$.toString();
            console.log(channel.toLowerCase()+':'+algo+':'+itemId+':'+version)
            similarityTable.row(channel.toLowerCase()+':'+algo.toLowerCase()+':'+itemId+':'+version).get(function(error, data) {
                if (data) {
                    data.forEach(function(d) {
                        columnData = d.column.toString().split(':');
                        score = parseInt(columnData[1]);
                        ritemId = parseInt(columnData[2]);            
                        similarItems.push([ritemId, score]);
                    });
                    
                    similarItems.sort(function(a,b ) { return b[1] - a[1]; })
                    similarItems = similarItems.slice(0, num)
                    
                    response = []
                    similarItems.forEach(function(similarItem) {
                        response.push({'item': similarItem[0], 'score': similarItem[1]})
                    })
                    res.json(response)
                }
                else {
                    res.json([])
                }
            });
        }
        else {
            
            res.json([])
        }
    })
});

router.post('/addItemMetaData', function(req, res) {
    var itemId = req.body.itemId;
    var channel = req.body.channel;
    var attributes = req.body.attributes;
    
    console.log(itemId);
    console.log(channel);
    console.log(attributes);
    
    var esclient = new elasticsearch.Client({
        host: 'localhost:9200'
    });
    
    var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 })
    var channelTable = hclient.table('channels');
    var attributeTable = hclient.table('contentBasedAttributes');
    
    attributeTable.row(channel+':*').get(function(error, data) {
        attributeResults = {};
        data.forEach(function(d) {
            rowKey = d.key.toString();
            rowKeyParts = rowKey.split(':')
            attributeKey = rowKeyParts[1]
            if (attributeKey) {
                if (!(attributeKey in attributeResults)) {
                    attributeResults[attributeKey] = {};
                }
                columnData = d.column.toString().split(':');
                if (columnData[0] == 'info') {
                    columnName = columnData[1];
                    columnVal = d.$.toString();
                    attributeResults[attributeKey][columnName] = columnVal;
                }    
            }
        })
        //console.log(attributeResults)
        
        isDataValid = true;
        finalData = {}
        
        for (attributeKey in attributes) {
            if (attributeResults.hasOwnProperty(attributeKey)) {
                attributeType = attributeResults[attributeKey]['type'];
                if (attributeType == 'numeric') {
                    attributeValue = parseFloat(attributes[attributeKey]);
                }
                else {
                    attributeValue = attributes[attributeKey];
                }
                finalData[attributeKey] = attributeValue;
            }
            else {
                console.log(attributeKey)
                isDataValid = false;
            }
        }
        
        finalData['channelId'] = channel;
        finalData['itemId'] = itemId;
        documentId = murmurhash(channel+":"+itemId);
        
        if (isDataValid) {
            esclient.index({
                index: 'content_based_data2',
                type: 'data',
                id: documentId,
                body: finalData
            }, function (error, response) {
                console.log(response);
            });
        }
        
        res.json([])
    })
})

router.post('/record', function(req, res) {

    var userId = parseInt(req.body.userid); 
    var itemId = parseInt(req.body.itemid); 
    var datetime = req.body.time;
    var action = req.body.action;
    var channel = req.body.channel;
    var clientId = 'CL-150EB497B6F'
    
    console.log(channel+","+userId+","+itemId+","+datetime+","+action);
    
    messages = [];
    messages.push(clientId+","+channel+","+userId+","+itemId+","+datetime+","+action);
    payloads = [{topic: 'c101.topic', messages: messages, partition: 0}]
    //kafkaProducer.on('ready', function(){
        kafkaProducer.send(payloads, function(err, data){
            console.log(data);
            res.json([]);
        });
    //});
    kafkaProducer.on('error', function(err){ console.log(err); });
    
    
    
    //module 24    
    //var hclient = new hbase.Client({ host: '127.0.0.1', port: 8023 });
    //var channelTable = hclient.table('channels');
    //
    //channelTable.row(channel).get('info:client', function(error, data) {
    //    if (data) {
    //        var clientId = data[0].$.toString();
    //        messages = [];
    //        messages.push(clientId+","+channel+","+userId+","+itemId+","+datetime+","+action);
    //        payloads = [{topic: 'c101.topic', messages: messages, partition: 0}]
    //        //kafkaProducer.on('ready', function(){
    //            kafkaProducer.send(payloads, function(err, data){
    //                console.log(data);
    //                res.json([]);
    //            });
    //        //});
    //        kafkaProducer.on('error', function(err){ console.log(err); });     
    //    }
    //    else {
    //        res.json([]);
    //    }
    //});
});

module.exports = router;
