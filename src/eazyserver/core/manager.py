import logging
logger = logging.getLogger(__name__)
logger.debug("Loaded " + __name__)

import os
import sys
import signal

from kafka_connector import KafkaConnector

class Manager(object):
	Type = "Manager"

	def __init__(self, **kwargs):
		super(Manager, self).__init__()

		self.behaviour = kwargs.get('behaviour')
		self.connector_type = kwargs.get('connector_type')
		self.kafka_client_type = kwargs.get('kafka_client_type')
		self.kafka_client_config = kwargs.get('kafka_client_config')

		self.connected_behaviour = KafkaConnector(
			self.behaviour, 
			kafka_client_type=self.kafka_client_type, 
			kafka_client_config=self.kafka_client_config)


	def run(self):
		logger.info("Manager run() called.")
		self.connected_behaviour.run()

	def onStart(self):
		logger.info("Manager onStart() called.")

	def onExit(self):
		logger.info("Manager onExit() called.")

	# Handling Signals
	def receiveSignal(self, signal_number, frame):  
	    print('Received:', signal_number)

	    # SIGUSR1
	    if(signal_number == 10):
	    	logger.info("Signal - SIGUSR1")
	    	self.behaviour.signal_method_usersignal1()

	    # SIGUSR2
	    if(signal_number == 12):
	    	logger.info("Signal - SIGUSR2")
	    	self.behaviour.signal_method_usersignal2()

	    # SIGTERM
	    if(signal_number == 15):
	    	logger.info("Terminating...")
	    	self.behaviour.onExit()

	    # SIGHUP
	    if(signal_number == 1):
	    	logger.info("Received SIGHUP")
	    	self.behaviour.reload_config()
	    

	def onSignal(self):
		logger.info("Manager Signal Handler Initialized.")
		logger.info('My PID is:{}'.format(str(os.getpid())))

		signal.signal(signal.SIGUSR1, self.receiveSignal)
		signal.signal(signal.SIGUSR2, self.receiveSignal)
		signal.signal(signal.SIGTERM, self.receiveSignal)
		signal.signal(signal.SIGHUP, self.receiveSignal)




