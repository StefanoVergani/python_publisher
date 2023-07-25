from confluent_kafka import Producer
import json
import os
# this is a first attempt to convert the ERSPublisher.cpp into Python
class ERSPublisher:

    def __init__(self, conf):

        self.default_topic = 'ers_stream'

        if 'bootstrap' in conf:
            bootstrap = conf['bootstrap']
        else:
            raise ValueError("Missing bootstrap from json file")

        if 'client_id' in conf:
            client_id = conf['client_id']
        elif 'DUNEDAQ_APPLICATION_NAME' in os.environ:
            client_id = os.environ['DUNEDAQ_APPLICATION_NAME']
        else:
            client_id = 'erskafkaproducerdefault'

        self.producer = Producer({'bootstrap.servers': bootstrap, 'client.id': client_id})

        if 'default_topic' in conf:
            self.default_topic = conf['default_topic']


    def publish(self, issue):

        binary = issue.SerializeToString()  # Assuming issue is an instance of a protobuf Message

        # get the topic
        topic = self.get_topic(issue)  

        # get the key
        key = self.get_key(issue)  

        err = self.producer.produce(topic, binary, key)

        if err is not None:
            return False

        return True

    def get_topic(self, issue):
        # I need to replace this with actual logic to get topic from issue
        # For now, I use default_topic as per the C++ code
        return self.default_topic

    def get_key(self, issue):
        # I need to replace this with actual logic to get key from issue
        # For now, I assume issue has a session() method
        return issue.session()

