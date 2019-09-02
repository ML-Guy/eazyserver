import logging
logger = logging.getLogger(__name__)
logger.debug("Loaded " + __name__)

import json
import time
import sys
import pprint

from confluent_kafka import Producer as KafkaProducer
from confluent_kafka import Consumer as KafkaConsumer
from confluent_kafka import TopicPartition

class Kafka_Confluent(object):
	Type = "Confluent-Kafka Wrapper Class"
	def __init__(self, kafka_client_config):

		print("="*50)
		print("Printing Kafka_Confluent kwargs...")
		import pprint
		pp = pprint.PrettyPrinter(indent=4)
		pp.pprint(kafka_client_config)
		print("="*50)

		self.broker = kafka_client_config["broker"]
		self.producer_params = kafka_client_config["producer_params"]
		self.consumer_1_params = kafka_client_config["consumer_1_params"]
		self.consumer_2_params = kafka_client_config["consumer_2_params"]

		self.producer_topic = None
		self.consumer_1_topic = None
		self.consumer_2_topic = None

		self.producer = None
		self.consumer_1 = None
		self.consumer_2 = None

		# Create Producer
		if(kafka_client_config['producer_topic']):
			self.producer_topic = kafka_client_config['producer_topic']
			self.producer_params['bootstrap.servers'] = kafka_client_config["broker"]
			self.producer = KafkaProducer(self.producer_params)
			print("Producer created successfully...")

		# Create Consumer 1
		if(kafka_client_config['consumer_1_topic']):
			self.consumer_1_topic = kafka_client_config['consumer_1_topic']
			self.consumer_1_params['bootstrap.servers'] = kafka_client_config["broker"]
			self.consumer_1 = KafkaConsumer(self.consumer_1_params)
			self.consumer_1.subscribe([self.consumer_1_topic])
			self.consumer_1.poll(timeout=0.01)
			print("Consumer 1 created successfully...")

		# Create Consumer 2
		if(kafka_client_config['consumer_2_topic']):
			self.consumer_2_topic = kafka_client_config['consumer_2_topic']
			self.consumer_2_params['bootstrap.servers'] = kafka_client_config["broker"]
			self.consumer_2 = KafkaConsumer(self.consumer_2_params)
			self.consumer_2.subscribe([self.consumer_2_topic])
			self.consumer_2.poll(timeout=0.01)
			print("Consumer 1 created successfully...")

		# TODO : Print Complete config


	def produce(self, value):
		print("="*50)
		print("Producing Message")
		print("self.producer_topic", self.producer_topic)
		print("message size, ", str(len(value)))
		print("="*50)
		self.producer.produce(self.producer_topic, value)
		self.producer.poll(0)
		return(True)

	def consume1(self):
		print("="*50)
		print("Consuming Message")
		print("self.consumer_1_topic", self.consumer_1_topic)
		print("="*50)
		message_kafka = self.consumer_1.consume(num_messages=1)[0]
		message_dict = json.loads(message_kafka.value())
		return(message_dict)

	def consume2(self):
		print("="*50)
		print("Consuming Message")
		print("self.consumer_2_topic", self.consumer_2_topic)
		print("="*50)
		message_kafka = self.consumer_2.consume(num_messages=1)[0]
		message_dict = json.loads(message_kafka.value())
		return(message_dict)
		
	def sync_consumers(self):

		m1 = self.consumer_1.consume()[0]
		m2 = self.consumer_2.consume()[0]

		if(m1.offset() == m2.offset()): # Consumers are synced
			return(m1.value(), m2.value())

		logger.info("Syncing Consumers...")

		consumer_2_topic_name = m2.topic()
		consumer_2_partition = m2.partition()
		consumer_2_offset = m1.offset()
		consumer_2_topic_partition = TopicPartition(topic=consumer_2_topic_name, partition=consumer_2_partition, offset=consumer_2_offset) 

		# Sync Consumer 2
		self.consumer_2.seek(consumer_2_topic_partition)
		m2 = self.consumer_2.consume()[0]

		import pdb; pdb.set_trace()

		if(m1.offset() == m2.offset()): 
			return(m1.value(), m2.value())

		logger.info("Consumers not synced. Unknown error.")
		sys.exit(0)