# General Settings
import json

# Web Server specific imports
from japronto import Application
from json import JSONDecodeError
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=["35.154.159.4:9092", "35.154.159.4:9093", "35.154.159.4:9094"], retries=5)

def visuG(request):
	print(producer.config)
	print(producer)
	jstore = request.json
	print(jstore)	
	jevents = json.loads(jstore['e'])
	print(jevents)
	raw_data = {'topic': 'vnk-desd', 'routes': 'clst/desd', 'messages': jevents}
	f = producer.send("vnk-raw", json.dumps(raw_data).encode('ascii'))
	print(f)
	r = f.get(timeout=1)
	return request.Response(code=200,json=r)


app = Application()


# The Router instance lets you register your handlers and execute
# them depending on the url path and methods
r = app.router
r.add_route('/user-activity-poc', visuG, methods=["POST"])
r.add_route('/spcb', visuG, methods=["POST"])

# Finally start our server and handle requests until termination is
# requested. Enabling debug lets you see request logs and stack traces.
app.run(debug=True)