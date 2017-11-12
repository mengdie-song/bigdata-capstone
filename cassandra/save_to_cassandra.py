import atexit
import json
import logging

from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka.errors import KafkaError


# Server configuration
kafka_broker = 'localhost:9092'
cassandra_broker = 'localhost'

kafka_topic = 'coin_average'
cassandra_keyspace = 'coin_analysis'
cassandra_table = 'cost_average'

# set the logger
logging.basicConfig()
logger = logging.getLogger('save_to_cassandra')
logger.setLevel(logging.DEBUG)


def persist_data(kafka_msg, cassandra_session):
	try:
		data = json.loads(kafka_msg.value)
		coin_symbol = data['symbol']
		price_type = data['price_type']
		average = float(data['average'])
		write_time = data['write_time']

		insert_statement = "INSERT INTO %s (coin_symbol, price_type, average, write_time) VALUES ('%s', '%s', %f, '%s');" % (cassandra_table, coin_symbol, price_type, average, write_time)
		cassandra_session.execute(insert_statement)
		logger.info('Persistend data to cassandra for symbol: %s, write_time: %s, average: %f' % (coin_symbol, write_time, average))
	except Exception as e:
		logger.error('error occur: %s', e.message)


def shutdown_hook(kafka_consumer, cassandra_session):
	try:
		logger.info('Closing Kafka Consumer')
		kafka_consumer.close()
		logger.info('Kafka Consumer closed')

		logger.info('Closing Cassandra Session')
		cassandra_session.shutdown()
		logger.info('Cassandra Session closed')
	except KafkaError as kafka_error:
		logger.warn('Failed to close Kafka Consumer, caused by: %s', kafka_error.message)
	finally:
		logger.info('Existing program')


if __name__ == '__main__':
	# Initialize the kafka consumer to read from
	kafka_consumer = KafkaConsumer(
		kafka_topic,
		bootstrap_servers=kafka_broker
	)

	# Initialize cassandra
	cassandra_cluster = Cluster(
		contact_points=cassandra_broker.split(',')
	)
	cassandra_session = cassandra_cluster.connect()

	# create the keyspace and table if not exist
	cassandra_session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = 'true'" % cassandra_keyspace)
	cassandra_session.set_keyspace(cassandra_keyspace)
	cassandra_session.execute("CREATE TABLE IF NOT EXISTS %s (coin_symbol text, price_type text, average float, write_time text, PRIMARY KEY (coin_symbol, price_type, write_time))" % cassandra_table)

	atexit.register(shutdown_hook, kafka_consumer, cassandra_session)

	for kafka_msg in kafka_consumer:
		persist_data(kafka_msg, cassandra_session)