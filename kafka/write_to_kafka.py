import atexit
import csv
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from apscheduler.schedulers.background import BackgroundScheduler


# Server configuration
kafka_broker = 'localhost:9092'

# Raw data
log_path = '/home/bigdata/data-pipeline/crypto-markets.csv'

# Kafka topic
kafka_topic = 'coin'

# set the logger
logging.basicConfig()
logger = logging.getLogger('write_to_kafka')
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


def write_kafka(kafka_producer):
	with open(log_path, 'rb') as log_file:
		log_reader = csv.DictReader(log_file)
		for row in log_reader:
			price_info = json.dumps(row)
			try:
				kafka_producer.send(topic=kafka_topic, value=price_info)
				logger.debug('Sent price info for %s: %s to Kafka', row['symbol'], price_info)
			except KafkaError as ke:
				logger.warn('Failed to send price info for %s: %s to Kafka', row['symbol'], price_info)


if __name__ == '__main__':
	# Open the kafka producer
	kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)

	# Release the resources when exitting
	atexit.register(shutdown_hook, kafka_producer)

	write_kafka(kafka_producer)