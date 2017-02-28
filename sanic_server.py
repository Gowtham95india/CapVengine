#!/usr/bin/python3
import ujson


from sanic import Sanic
from sanic.response import json as json_parser
from urllib.parse import unquote, parse_qs

app = Sanic()

@app.route("/stats", methods=['POST'])
@app.route("/user-activity-poc", methods=['POST'])
async def StatsCollector(request):
	try:
		jstore = request.json
		jevents = ujson.loads(jstore['e'])

	except Exception as e:
		try:
			jstore = parse_qs(unquote(request.body.decode('utf-8').replace("'",'"')))
			jevents = ujson.loads(jstore['e'][0])
		except Exception as er:
			return json_parser(status=422, body={"status": False, "message": "Unparsble JSON"})

	raw_data = {'topic': 'vnk-clst', 'routes': 'clst/android', 'messages': jevents}
	producer.send("vnk-raw", raw_data)

	return json_parser({"status": True, "message": "OK"})

@app.route("/fireme", methods=['POST'])
async def VigeonCollector(request):
	try:
		jevents =request.json

	except JSONDecodeError:
		return json_parser(status=422, body={"status": False, "message": "Unparsble JSON"})

	raw_data = {'topic': 'vnk-clst', 'routes': 'clst/vigeon', 'messages': [jevents]}
	producer.send("vnk-raw", raw_data)

	return json_parser({"status": True, "message": "OK"})

@app.route("/spcb", methods=["POST"])
async def EventsCollector(request):
	try:
		jevents = request.json

	except JSONDecodeError:
		return json_parser(status=422, body={"status": False, "message": "Unparsble JSON"})

	raw_data = {'topic': 'vnk-desd', 'routes': 'clst/desd', 'messages': jevents}
	producer.send("vnk-raw", raw_data)

	return json_parser({"status": True, "message": "OK"})


if __name__ == "__main__":
	
	app.run(host="0.0.0.0", port=8000, workers=4, debug=True)
