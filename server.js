var nr = require('newrelic');
var express  = require('express');
app = express();
var port = process.env.PORT || 8080;
var bodyParser = require('body-parser');
var fs=require("fs");

var Converter = require("csvtojson").Converter;
var converter = new Converter({});

app.use(bodyParser.json());     // support json encoded bodies
app.use(bodyParser.urlencoded({ extended: false }));    // support encoded bodies

var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client(),
    producer = new Producer(client);

app.listen(port);
console.log('Server started! At http://localhost:' + port);

app.post('/user-activity-poc',function(req, res) {

    // console.log(req.body);
    var date = new Date().toISOString().toString('utf8');
    try {
        var store = JSON.parse(JSON.stringify(req.body).toString('utf8').replace("'",'"'));
        store = JSON.parse(store.e); // Getting events list
    }
    catch (e) {
        var store = [];
        console.log("Error in JSON Parsing!");
        res.status(400);
    }

    payloads = [];

    for (eve=0;eve<store.length;eve++){
        // Only realtime events. Convert timestamp to ISOstring format.
        store[eve].timestamp = new Date(store[eve].timestamp * 1000).toISOString().toString('utf8');
        //Uncomment the following just in case to capture older events.
        // store[eve].timestamp = new Date().toISOString().toString('utf8');

        // Adding event_day IST and UTC format.
        var currentUTCTime = new Date();
        var currentISTTime = new Date(currentUTCTime.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
        store[eve].event_day = currentUTCTime.toLocaleString().split(',')[0];
        store[eve].event_day_ist = currentISTTime.toLocaleString.split(',')[0];

        temp_obj = { topic: "vnk-clst", messages: JSON.stringify(store[eve]), partition: 0 };
        payloads.push(temp_obj);
    }

    producer.send(payloads, function(err, data){
        console.log(data);
    });

    producer.on('error', function(err){
        console.log(err); 
        res.status(500);
    })
    res.end();
});


app.post('/fireme',function(req, res) {

    // console.log(req.body);
    var date = new Date().toISOString().toString('utf8');
    try {
        var store = JSON.parse(JSON.stringify(req.body).toString('utf8').replace("'",'"'));
    }
    catch (e) {
        var store = [];
        console.log("Error in JSON Parsing!");
    }

    payloads = [];

    // Only realtime events. Convert timestamp to ISOstring format.
    // store[eve].timestamp = new Date(store[eve].timestamp * 1000).toISOString().toString('utf8');
    store.timestamp = date;

        // Adding event_day IST and UTC format.
    var currentUTCTime = new Date();
    var currentISTTime = new Date(currentUTCTime.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    store.event_day = currentUTCTime.toLocaleString().split(',')[0];
    store.event_day_ist = currentISTTime.toLocaleString.split(',')[0];

    temp_obj = { topic: "vnk-clst", messages: JSON.stringify(store), partition: 0 };
    payloads.push(temp_obj);

    producer.send(payloads, function(err, data){
            console.log(data);  
    });

    producer.on('error', function(err){
        console.log(err); 
        res.status(500);
    })
    res.end();
});