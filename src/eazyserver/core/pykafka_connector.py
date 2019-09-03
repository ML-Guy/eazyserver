import logging
logger = logging.getLogger(__name__)
logger.debug("Loaded " + __name__)

import json
import time
import sys
import pprint

from pykafka import KafkaClient
from pykafka.common import OffsetType

class Kafka_PyKafka(object):
	Type = "PyKafka Wrapper Class"
	def __init__(self, kafka_client_config):

		# Get Config Params
		self.broker = kafka_client_config["broker"]
		self.producer_topic = kafka_client_config.get("producer_topic")
		self.consumer_1_topic = kafka_client_config.get("consumer_1_topic")
		self.consumer_2_topic = kafka_client_config.get("consumer_2_topic")

		# Create global (sort of) PyKafka client
		self.pykafka_client = KafkaClient("kafka:9092")

		# Create Producer
		if(self.producer_topic):
			topic = self.pykafka_client.topics[kafka_client_config["producer_topic"]]
			self.producer = topic.get_producer(kafka_client_config["producer_params"])


		# Create Consumer 1
		if(self.consumer_1_topic):
			topic = self.pykafka_client.topics[self.consumer_1_topic]
			self.consumer_1 = topic.get_simple_consumer(kafka_client_config["consumer_1_params"])

		# Create Consumer 2
		if(self.consumer_2_topic):
			topic = self.pykafka_client.topics[self.consumer_2_topic]
			self.consumer_2 = topic.get_simple_consumer(kafka_client_config["consumer_2_params"])

		# Print Complete config

	def produce(self, message):
		self.producer.produce(message)

	def consume1(self):
		message_kafka = self.consumer_1.consume()
		if(message_kafka is not None):
			
			print("="*50)
			print("Consumed 1 PyKafka")
			print("Message Size = ".format(str(len(message_kafka))))
			print("="*50)

			# Duplicate code converting Binary to final Dict message
			the_binary = message_kafka.value
			jsn = ''.join(chr(int(x, 2)) for x in the_binary.split())
			msg = json.loads(jsn)
			# kafka_msg_id = "{id}:{topic}:{partition}:{offset}".format(**{ "id":msg["_id"],"offset":kafka_msg.offset(), "partition": kafka_msg.partition(), "topic":kafka_msg.topic() })
			# msg["_kafka__id"]= kafka_msg_id
			
			return(msg)
		else:
			logger.info("Empty message received from consumer")
			return(None)

	def consume2(self):
		message_kafka = self.consumer_2.consume()
		if(message_kafka is not None):
			
			print("="*50)
			print("Consumed 2 PyKafka")
			print("Message Size = ".format(str(len(message_kafka))))
			print("="*50)

			# Duplicate code converting Binary to final Dict message
			the_binary = message_kafka.value
			jsn = ''.join(chr(int(x, 2)) for x in the_binary.split())
			msg = json.loads(jsn)
			return(msg)
		else:
			logger.info("Empty message received from consumer")
			return(None)


	def sync_consumers(self):

		message_1 = self.consumer_1.consume()
		message_1_offset = message_1.offset - 2
		message_1_partition = self.consumer_1.partitions[0]
		offset = [(message_1_partition, message_1_offset)]

		self.consumer_1.reset_offsets([(message_1_partition, message_1_offset)])
		self.consumer_1.stop()
		self.consumer_1.start()

		self.consumer_2.reset_offsets([(message_1_partition, message_1_offset)])
		self.consumer_2.stop()
		self.consumer_2.start()

		m1 = self.consumer_1.consume()
		m2 = self.consumer_2.consume()

		print("Synced????")

		print("OFFSET M1 = {}".format(m1.offset))
		print("OFFSET M2 = {}".format(m2.offset))

		if(m1.offset == m2.offset):
			return(m1.value, m2.value)