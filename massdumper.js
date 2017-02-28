var nr = require('newrelic');
var express  = require('express');
app = express();
var port = process.env.PORT || 8080;
var bodyParser = require('body-parser');

app.use(bodyParser.json());     // support json encoded bodies
app.use(bodyParser.urlencoded({ extended: false }));    // support encoded bodies

var kafka = require('kafka-node'),
    Producer = kafka.HighLevelProducer,
    client = new kafka.Client("35.154.145.181:2181"),
    producer = new Producer(client);

producer.on('ready', function () {
    console.log("Kafka is Ready. I can produce now!")
});

// This is the error event propagates from internal client.
producer.on('error', function(err){
    console.log("Internal Producer Error!" + err);
});

app.listen(port);
console.log('Server started! At http://localhost:' + port);

// If timestamp is not present, returns ISO format current timestamp.
var getTimeStamp = function(timestamp) {

    if(timestamp) {

        // Only realtime events. Convert timestamp to ISOstring format.
        // Timestamps can be in milliseconds/microseconds.
        if(timestamp > 100000000000000){
            timestamp = Math.round(timestamp/1000);
        }
        else if(timestamp > 100000000000){
            timestamp = Math.floor(timestamp);
        }
        else{
            timestamp = timestamp * 1000;
        }
        return timestamp;
    }
    else {
        return new Date().toISOString().toString('utf8');
    }
}

var statsCollector = function(req, res) {

	try {
        var store = JSON.parse(JSON.stringify(req.body).toString('utf8').replace("'",'"'));
        store = JSON.parse(store.e); // Getting events list
    }
    catch (e) {
        console.log("Error in JSON Parsing!");
        return res.status(422).json({"status":false, "message":"Unparsble JSON"}).end();
    }

    var raw_data = {'topic': 'vnk-clst', 'routes': 'clst/android', 'messages': store};
    var data_to_send = [{ topic: "vnk-raw", messages: JSON.stringify(raw_data)}];

    producer.send(data_to_send, function(err, data){
        console.log(data);
        if (err) return res.status(503).json({ "status": false, "message": "503 Service Unavailable", "error": err }).end();
        else return res.status(200).json({ "status": true, "message": "OK"}).end();
    });
};

var vigeonCollector = function(req, res) {

	try {
		var store = JSON.parse(JSON.stringify(req.body).toString('utf8').replace("'",'"'));
	}
	catch (e) {
        console.log("Error in JSON Parsing!");
        return res.status(422).json({"status":false, "message":"Unparsble JSON"});
    }

    var raw_data = {'topic': 'vnk-clst', 'routes': 'clst/vigeon', 'messages': [store]};
    var data_to_send = [{ topic: "vnk-raw", messages: JSON.stringify(raw_data)}];

    producer.send(data_to_send, function(err, data){
        console.log(data);
        if (err) return res.status(503).json({ "status": false, "message": "503 Service Unavailable", "error": err });
        else return res.status(200).json({ "status": true, "message": "OK"});
    });
}


var eventsCollector = function(req, res){

    var date = new Date().toISOString().toString('utf8');
    try {
        var store = JSON.parse(JSON.stringify(req.body).toString('utf8').replace("'",'"'));
    }
    catch (e) {
        console.log("Error in JSON Parsing!");
        return res.status(422).json({"status":false, "message":"Unparsble JSON"});
    }

    var raw_data = {'topic': 'vnk-desd', 'routes': 'clst/desd', 'messages': store};
    var data_to_send = [{ topic: "vnk-raw", messages: JSON.stringify(raw_data)}];

    producer.send(data_to_send, function(err, data){
        
        console.log(data);
        if (err) return res.status(503).json({ "status": false, "message": "503 Service Unavailable", "error": err });
        else return res.status(200).json({ "status": true, "message": "OK"});

    });

}

app.post('/user-activity-poc', statsCollector);
app.post('/stats', statsCollector);
app.post('/fireme', vigeonCollector);
app.post('/spcb', eventsCollector);
