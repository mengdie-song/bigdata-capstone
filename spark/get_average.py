# spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.2.0.jar get_average.py high

import argparse
import atexit
import logging
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


# Server configuration
kafka_broker = 'localhost:9092'

kafka_topic = 'coin'

new_kafka_topic = 'coin_average'

# set the logger
logging.basicConfig()
logger = logging.getLogger('get_average')
logger.setLevel(logging.DEBUG)


def shutdown_hook(kafka_producer):
	try:
		logger.info('Flushing pending messages to kafka, timeout is set to 10s')
		kafka_producer.flush(10)
		logger.info('Finish flushing pending messages to kafka')
	except KafkaError as kafka_error:
		logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
	finally:
		try:
			logger.info('Closing kafka connection')
			kafka_producer.close(10)
		except Exception as e:
			logger.warn('Failed to close kafka connection, caused by: %s', e.message)


def process_stream(stream, kafka_producer, price_type):

	def write_to_kafka(rdd):
		results = rdd.collect()
		for r in results:
			data = json.dumps(
				{
					'write_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
					'symbol':	  r[0],
					'price_type': price_type,
					'average': 	  r[1],
				}
			)
			logger.info('========================Sending average price to kafka========================')
			kafka_producer.send(topic=new_kafka_topic, value=data)

	def pair(data):
		record = json.loads(data[1].decode('utf-8'))
		return record['symbol'], (float(record[price_type]), 1)

	stream.map(pair).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])).map(lambda (k, v): (k, v[0]/v[1])).foreachRDD(write_to_kafka)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('price_type', help='The price type to analysis: open, high, low, close')

	args = parser.parse_args()
	price_type = args.price_type

	# create SparkContext and StreamingContext
	sc = SparkContext("local[2]", "AveragePrice1")
	sc.setLogLevel('info')
	ssc = StreamingContext(sc, 3)

	# Initialize output Kafka producer
	kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)

	# Initialize input Kafka stream
	directKafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {'metadata.broker.list': kafka_broker})
	process_stream(directKafkaStream, kafka_producer, price_type)

	atexit.register(shutdown_hook, kafka_producer)

	ssc.start()
	ssc.awaitTermination()