import logging
logger = logging.getLogger(__name__)
logger.debug("Loaded " + __name__)

import json
import time
import pprint
from bson.objectid import ObjectId
from datetime import datetime

from confluent_kafka import Producer as KafkaProducer
from confluent_kafka import Consumer as KafkaConsumer
from confluent_kafka import TopicPartition

from pykafka import KafkaClient
from pykafka.common import OffsetType

#############################
## Helper Methods
#############################

def dict_to_binary(the_dict):
	binary = ' '.join(format(ord(letter), 'b') for letter in the_dict)
	return binary

def binary_to_dict(the_binary):
	jsn = ''.join(chr(int(x, 2)) for x in the_binary.split())
	return jsn

def kafka_to_dict(kafka_msg):
	msg = json.loads(binary_to_dict(kafka_msg.value))
	kafka_msg_id = "{id}:{topic}:{partition}:{offset}".format(**{ "id":msg["_id"],"offset":kafka_msg.offset(), "partition": kafka_msg.partition(), "topic":kafka_msg.topic() })
	msg["_kafka__id"]= kafka_msg_id
	return msg
	
def dict_to_kafka(output,source_data):
	for data in source_data:
		if output["source_id"] == data["_id"]:
			output["_kafka_source_id"] = data["_kafka__id"]
			break
	kafka_msg = dict_to_binary(json.dumps(output))
	return kafka_msg

# TODO: Move/Add formatOutput to behaviour base class 
# Created following fields in output dict if missing:
# _id,_created,_updated,source_id,_type,_producer
def formatOutput(output,behavior,source_data): 
	if "_id" not in output: output["_id"] = str(ObjectId())
	if "_updated" not in output: output["_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	if "_type" not in output: output["_type"] = "BEHAVIOUR"		#TODO take from behavior object
	if "_producer" not in output: output["_producer"] = "{}:{}:{}".format(behavior.__class__.__name__,"1.0",behavior.id) #name:version:id #TODO take version from behaviour

	# Source chaining for stream
	if "source_id" not in output: 
		if source_data: # Select rightmost consumer
			output["source_id"] = source_data[-1]["_id"]
		else:  # This is Producer
			output["source_id"] = output["_id"]
	if "_created" not in output: 
		if output["source_id"] is None or output["source_id"] == output["_id"]:
			output["_created"] = output["_updated"]
		else:
            # Propagate _created from input data which is source (_id of input specified as source_id of output)
			for data in source_data:
				if output["source_id"] == data["_id"]:
					output["_created"] = data["_created"]
					break
            # Propagate _created time based upon same source_id of input data
			for data in source_data:
				if output["source_id"] == data["source_id"]:
					output["_created"] = data["_created"]
					break
                    
	if "_created" not in output: 		
		logger.info("{} | source_id  {} not found for id {}".format(output["_producer"],output["source_id"],output["_id"]))
		output["_created"] = output["_updated"]
		
	return output

#############################
## Kafka Library classes
#############################

class Kafka_PyKafka(object):
	Type = "PyKafka Wrapper Class"
	def __init__(self, kafka_client_config):

		# Get Config Params
		self.broker = kafka_client_config["broker"]
		self.producer_topic = kafka_client_config["producer_topic"]
		self.consumer_1_topic = kafka_client_config["consumer_1_topic"]
		self.consumer_2_topic = kafka_client_config["consumer_2_topic"]
		self.sync_consumers = kafka_client_config["sync_consumers"]

		# Create global (sort of) PyKafka client
		self.pykafka_client = KafkaClient("kafka:9092")

		# Create Producer
		if(kafka_client_config["producer_topic"]):
			topic = self.pykafka_client.topics[kafka_client_config["producer_topic"]]
			self.producer = topic.get_producer(kafka_client_config["producer_params"])


		# Create Consumer 1
		if(kafka_client_config["consumer_1_topic"]):
			topic = self.pykafka_client.topics[kafka_client_config["consumer_1_topic"]]
			self.consumer_1 = topic.get_simple_consumer(kafka_client_config["consumer_1_params"])

		# Create Consumer 2
		if(kafka_client_config["consumer_2_topic"]):
			topic = self.pykafka_client.topics[kafka_client_config["consumer_2_topic"]]
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
			# import pdb; pdb.set_trace()
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
			# kafka_msg_id = "{id}:{topic}:{partition}:{offset}".format(**{ "id":msg["_id"],"offset":kafka_msg.offset(), "partition": kafka_msg.partition(), "topic":kafka_msg.topic() })
			# msg["_kafka__id"]= kafka_msg_id
			# import pdb; pdb.set_trace()
			return(msg)
		else:
			logger.info("Empty message received from consumer")
			return(None)


	def sync_consumers(self):
		pass


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
		self.sync_consumers = kafka_client_config["sync_consumers"]
		self.producer_params = kafka_client_config["producer_params"]
		self.consumer_1_params = kafka_client_config["consumer_1_params"]
		self.consumer_2_params = kafka_client_config["consumer_2_params"]
		self.producer = None
		self.producer_topic = None
		self.consumer_1 = None
		self.consumer_1_topic = None
		self.consumer_2 = None

		# Create Producer
		if(self.producer_params):
			self.producer_topic = self.producer_params['topic']
			self.producer_params.pop('topic')
			self.producer = KafkaProducer(self.producer_params)
			print("Producer created successfully...")

		# Create Consumer 1
		if(self.consumer_1_params):
			self.consumer_1_topic = self.consumer_1_params['topic']
			self.consumer_1_params.pop('topic')

			self.consumer_1_params['bootstrap.servers'] = kafka_client_config["broker"]
			self.consumer_1 = KafkaConsumer(self.consumer_1_params)
			self.consumer_1.subscribe([self.consumer_1_topic])
			self.consumer_1.poll()

		# Create Consumer 2
		if(self.consumer_2_params):
			logger.info("self.consumer_2_params is not None!!!!!!!!!!!")
			self.consumer_2 = KafkaConsumer(self.consumer_2_params)
			self.consumer_2.subscribe([self.consumer_2_params['topic']])
			self.consumer_2.poll()

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
		print("Consumeing Message")
		print("self.consumer_1_topic", self.consumer_1_topic)
		print("="*50)
		message_kafka = self.consumer1.consume(num_messages=1)[0]
		message_dict = kafka_to_dict(message_kafka)
		return(message_dict)

	def consume2(self):
		message_kafka = self.consumer2.poll(timeout=0.01)

		if(message_kafka is not None):
			message_dict = kafka_to_dict(message_kafka)

		return(message_kafka)
		

	def sync_consumers(self):
		logger.info("Syncing consumers...")
		pass

#############################
## Main Connector Class
#############################

class KafkaConnector(object):
	Type = "KafkaConnector"

	def __init__(self, Behaviour, **kwargs):

		self.client = None
		self.behavior = Behaviour

		self.kafka_client_type = kwargs.get("kafka_client_type")
		self.kafka_client_config = kwargs.get("kafka_client_config")
		
		# TODO : Validate **kwargs

		print("="*50)
		print("Printing kwargs...")
		for k,v in kwargs.items():
			print(k, v)
		print("="*50)

		# Create client based on type of Kafka Client specified
		if(self.kafka_client_type == "pykafka"):
			self.client = Kafka_PyKafka(kafka_client_config=self.kafka_client_config)

		print("GG")
		if(self.kafka_client_type == "confluent"):
			print("WP")
			self.client = Kafka_Confluent(kafka_client_config=self.kafka_client_config)

	def run(self):
		while(True):
			source_data = []

			############################
			# Consume
			############################

			message_1 = None
			message_2 = None
			output = None

			# if both consumers are specified
			if(self.client.consumer_2_topic):
				print("BOTH CONSUMER PRESENT")

				synced = None

				# TODO Sync Consumers
				if(self.kafka_client_config['sync_consumers']):
					# sync_consumer = True
					synced = self.client.sync_consumers()

					# If properly synced, consume messages
					if(synced):
						message_2 = self.client.consume2()
						message_1 = self.client.consume1()

						source_data.append(message_2)
						source_data.append(message_1)

						output = self.behavior.run(message_1, message_2)
					else:
						logger.info("Consumers not Synced.")

				else:
					# sync_consumer = False
					message_2 = self.client.consume2()
					message_1 = self.client.consume1()

					import pdb; pdb.set_trace();

			elif(self.client.consumer_1_topic):
				message_1 = self.client.consume1()
				source_data.append(message_1)
				output = self.behavior.run(message_1)
			else:
				output = self.behavior.run()

			# Transform output to fill missing fields
			if output:
				output=formatOutput(output, self.behavior, source_data)

			############################
			# Produce
			############################

			if(self.client.producer_topic):
				if(output):
					message_to_produce = dict_to_kafka(output, source_data)
					producer_response = self.client.produce(message_to_produce)

