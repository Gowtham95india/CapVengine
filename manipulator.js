var nr = require('newrelic');
var express  = require('express');
app = express();
var port = process.env.PORT || 8070;
var bodyParser = require('body-parser');
var fs = require("fs");
var freegeoip = require('node-freegeoip');

const Promise = require('bluebird');

var Redis = require('ioredis');
var redis = new Redis();

app.use(bodyParser.json());     // support json encoded bodies
app.use(bodyParser.urlencoded({ extended: false }));    // support encoded bodies

app.listen(port);
console.log('Server started! At http://localhost:' + port);

var kafka = require('kafka-node'),
    Producer = kafka.HighLevelProducer,
    client = new kafka.Client("35.154.145.181:2181"),
    producer = new Producer(client);

producer.on('ready', function () {
    console.log("Kafka is Ready. I can produce now!")
});

var options = {
  host: '35.154.145.181:2181',
  groupId: 'node-attributor',
  sessionTimeout: 15000,
  // An array of partition assignment protocols ordered by preference.
  // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
  protocol: ['roundrobin'],

  // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
  // equivalent to Java client's auto.offset.reset
  fromOffset: 'latest', // default

  // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
  outOfRangeOffset: 'earliest', // default
  migrateHLC: false,    // for details please see Migration section below
  migrateRolling: true
};

var kafka = require('kafka-node'),
    Consumer = kafka.ConsumerGroup,
    consumer = new Consumer(
        options,
        [
            'vnk-raw'
        ]
    );

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

var onMessage = function (message) {

    // console.log(req.body);
    // console.log(req.get('content-type'));
    var raw_json = message.value;
    var raw_data = JSON.parse(raw_json);
    var store = raw_data.messages;

     console.log(raw_data);

    // Adding event_day IST and UTC format.
    var currentUTCTime = new Date();
    var currentISTTime = new Date(currentUTCTime.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    var event_day = currentUTCTime.toLocaleString().split(',')[0];
    var event_day_ist = currentISTTime.toLocaleString().split(',')[0];

    var date = new Date().toISOString().toString('utf8');

    var payloads = [];

    if (raw_data.topic == "vnk-desd"){


        store.forEach(function(user_event){

            user_event.timestamp = getTimeStamp(); // Should be tagged with current timestamp.

            // Adding event_day IST and UTC format.
            user_event.event_day = event_day
            user_event.event_day_ist = event_day_ist

            payloads.push(JSON.stringify(user_event));

        });
    }

    else {

        try {
            device_id = store[0].device_id
        }
        catch(e){
            console.log("Device ID is invalid");
            return false;
        }

        redis.get(store[0].device_id).then(function update_user_event(jredis_result) {

            var redis_result = JSON.parse(jredis_result) || {};

            // variable user_event bounded to this call. Not possible with for loop.
            store.forEach(function(user_event) { // Dont' messup with this one.

                var timestamp = getTimeStamp(user_event.timestamp)
                user_event.timestamp = new Date(timestamp).toISOString().toString('utf8');
                // console.log(user_event.timestamp);

                // Uncomment the following just in case to capture older events.
                // user_event.timestamp = new Date().toISOString().toString('utf8'); // Setting timestamp to current time.

                // Sometimes event_properties is missing. Addding empty one if not present.!
                if(!user_event.event_properties){
                    user_event.event_properties = {};
                }

                // Tweaking for location data if lat is not present.
                if(!user_event.lat){
                     //clientIp = getClientAddress(req);
                     //getClientLocation(clientIp, function(resp) {  // Problem with freegeoip.net rate limit.

                     user_event.country = user_event.country || "IN";
                     user_event.region = user_event.region || ""
                     user_event.city = user_event.city || "";
                     user_event.lat = user_event.lat || 0.000000;
                     user_event.lng = user_event.lng || 0.000000;

                   //});
                    }

                    var medium = user_event.event_properties.utm_medium
                    var source = user_event.event_properties.utm_source
                    var campaign = user_event.event_properties.utm_campaign

                    // Correcting UTM Sources from App Event
                    if (!medium && !campaign && !source){
                      medium = source = campaign = "Direct"
                    }
                    else{
                      medium = medium || source || campaign || "Direct"
                      source = source || medium || campaign
                      campaign = campaign || medium || source
                    }


                    if(user_event.event_type == "Session-Started") {

                        // Redis data should be updated with current app session details.
                        redis_result.medium = user_event.event_properties.utm_medium = medium;
                        redis_result.source = user_event.event_properties.utm_source = source;
                        redis_result.campaign = user_event.event_properties.utm_campaign = campaign;
                        redis_result.user_id = user_event.user_id = user_event.user_id || redis_result.user_id;
                        redis_result.email = user_event.email = user_event.email || redis_result.email;
                        redis_result.advertiser_id = user_event.advertiser_id || redis_result.advertiser_id;

                        // No need to write for every event Except this and the one below.
                        redis.set(user_event.device_id, JSON.stringify(redis_result)); // Never expired details about user.

                    }
                    else {

                        user_event.event_properties.utm_medium = redis_result.medium || medium;
                        user_event.event_properties.utm_source = redis_result.source || source;
                        user_event.event_properties.utm_campaign = redis_result.campaign || campaign;
                        user_event.user_id = user_event.user_id || redis_result.user_id || 0;
                        user_event.email = user_event.email || redis_result.email || "";

                        if (user_event.event_type == "NEW_APP_INSTALLS" || user_event.event_type == "UNINSTALL") {

                            // Helps in deciding the uninstalls attributions %.
                            redis_result.user_installed_medium = redis_result.medium || medium;
                            redis_result.user_installed_source = redis_result.source || source;
                            redis_result.user_installed_campaign = redis_result.campaign || campaign;

                            // No need to write for every event Except this and the one above.
                            redis.set(user_event.device_id, JSON.stringify(redis_result)); // Never expired details about user.

                        }
                    }

                    user_event.event_day = event_day;
                    user_event.event_day_ist = event_day_ist;
                    user_event.advertiser_id_met = user_event.advertiser_id;
                    user_event.device_id_met = user_event.device_id;
                    user_event.seller_met = user_event.event_properties.Seller;
                    user_event.brand_met = user_event.event_properties['Brand Name'];
                    user_event.product_size_met = user_event.event_properties.Size;

                    temp_obj = JSON.stringify(user_event)
                    payloads.push(temp_obj);

            });

        var data_to_send = [{ topic: raw_data.topic, messages: payloads}];

        producer.send(data_to_send, function(err, data){
            console.log(data);
            if (err) return redis.set('incr','failed_count')
        });


        });
    }

    

}
consumer.on('message', onMessage);
app.post('/stats', onMessage);