var express = require('express');
var path = require('path');
var favicon = require('static-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var multer = require('multer');
var kafka = require('kafka-node');

var routes = require('./routes/index');
var users = require('./routes/users');

var app = express(); 

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');

app.use(favicon());
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded());
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));
app.use(multer({ dest: './uploads/'}));

app.use('/', routes);
app.use('/about', routes);
app.use('/algos', routes);
app.use('/contact', routes);
app.use('/bigMenu', routes);
app.use('/addClient', routes);
app.use('/saveClient', routes);
app.use('/client', routes);
app.use('/channel', routes);
app.use('/dataBrowser', routes);
app.use('/getDataIngestionChartData', routes);
app.use('/addChannel', routes);
app.use('/saveChannel', routes);
app.use('/editChannelInfo', routes);
app.use('/contentBasedAttributes', routes);
app.use('/addContentBasedAttribute', routes);
app.use('/deleteAttribute', routes);
app.use('/addItemMetaData', routes);
app.use('/viewData', routes);
app.use('/attributeCoverage', routes);
app.use('/addAlgo', routes);
app.use('/deleteAlgo', routes);
app.use('/channelSettings', routes);
app.use('/bulkUploads', routes);
app.use('/jobs', routes);
app.use('/runJob', routes);
app.use('/jobHistory', routes);
app.use('/celery', routes);
app.use('/testAPI', routes);
app.use('/recommendations', routes);
app.use('/userHistory', routes);
app.use('/getSimilarItems', routes);
app.use('/bulkUpload', routes);
app.use('/saveBulkUpload', routes);
app.use('/similaritems', routes);
app.use('/about', routes);
app.use('/add', routes);
app.use('/createPoll', routes);
app.use('/users', users);

/// catch 404 and forwarding to error handler
app.use(function(req, res, next) {
    var err = new Error('Not Found');
    err.status = 404;
    next(err);
});

/// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
    app.use(function(err, req, res, next) {
        res.status(err.status || 500);
        console.error(err.stack);
        res.render('error', {
            message: err.message,
            error: err
        });
    });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
        message: err.message,
        error: {}
    });
});

//kafkaProducerObj = kafka.Producer;
//kafkaClientObj = new kafka.Client('localhost:2181');
//global.kafkaProducer = (global.kafkaProducer ? global.kafkaProducer : new kafkaProducerObj(kafkaClientObj));

//module.exports = app;

var debug = require('debug')('my-application');
//var app = require('../app');

app.locals.inspect = require('util').inspect;

app.set('port', process.env.PORT || 3000);

var server = app.listen(app.get('port'), function() {
  debug('Express server listening on port ' + server.address().port);
});
