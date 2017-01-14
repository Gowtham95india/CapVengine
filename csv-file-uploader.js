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

app.post('/post-app-data', function(req, res) {

    console.log(req.body);
    var date = new Date().toISOString().toString('utf8');
    var store = JSON.parse(JSON.stringify(req.body).toString('utf8').replace("'",'"'));

    store["timestamp"]=date;

    console.log(store);

    payloads = [ { topic: "marketing", messages: JSON.stringify(store), partition: 0 }, ];
    
    producer.on('ready', function(){
            producer.send(payloads, function(err, data){
                    console.log(data);
                });
            });

    producer.on('error', function(err){console.log(err); })

    res.end();
});

app.post('/post-batch-data', function(req, res) {

	    var csvFileName="../imply-1.3.0/quickstart/marketing.csv";

  	    converter.on("record_parsed", function (jsonObj) {
	    var date = new Date().toISOString().toString('utf8');
	    var store = jsonObj;
	
	    store["timestamp"]=date;
	    console.log(store);
	     payloads = [ { topic: 'in-app-batch', messages:JSON.stringify(store), partition: 0 }, ];
			producer.send(payloads, function(err, data){
				console.log(data);
				});
	    });	 
      fs.createReadStream(csvFileName).pipe(converter);

    producer.on('error', function(err){ console.log(err);})

    res.end();
});

app.get('/upload',function(req, req){

    var kafka = require('kafka-node'),
        Consumer = kafka.Consumer,
        client = new kafka.Client(),
        consumer = new Consumer(client,
            [
             	{ topic: 'marketing', partition: 0 }
            ],
            {
                autoCommit: false
            }
	);
	consumer.on('message', function (message) {
                console.log(message);
            });

        response.end();
});
