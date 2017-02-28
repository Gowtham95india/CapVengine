from sanic.response import json
from sanic import Blueprint

from kafka import KafkaProducer
from kafka.errors import KafkaError

kafka_bp = Blueprint('kafka_bp')


@kafka_bp.listener('before_server_start')
async def setup_connection(app, loop):
	global producer
	producer = KafkaProducer(bootstrap_servers=["35.154.159.4:9092", "35.154.159.4:9093"], retries=5, batch_size=0, value_serializer=lambda m: ujson.dumps(m).encode('ascii'))