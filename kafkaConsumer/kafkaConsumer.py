# Imports
import boto3
from kafka import TopicPartition, OffsetAndMetadata, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from aws_schema_registry import SchemaRegistryClient, DataAndSchema
from aws_schema_registry.avro import AvroSchema
from aws_schema_registry.adapter.kafka import KafkaDeserializer

# import logging
# logging.basicConfig(level=logging.DEBUG)


# Variables
# --------------------------------------------------------------------------
REGISTRY_NAME = 'GlueSchemaRegistry'
BOOTSTRAP_SERVER = 'b-1.mskworkshopcluster.6pux1a.c3.kafka.us-east-2.amazonaws.com'
KAFKA_TOPIC = 'gsr-kafka-topic' 
# --------------------------------------------------------------------------

try:
    # Creating  Glue client from the boto3 and Glue schema registry client
    glue_client = boto3.client('glue')
    gsr_client = SchemaRegistryClient(glue_client,registry_name=REGISTRY_NAME)

    # Creates the deserializer for Kafka consumer
    gsr_deserializer = KafkaDeserializer(client=gsr_client)

    #Kafka Consumer object using Glue schema Registry deserializer
    kafka_consumer = KafkaConsumer (KAFKA_TOPIC,bootstrap_servers=BOOTSTRAP_SERVER, 
                                    value_deserializer=gsr_deserializer,group_id='aws-glue-schema-registry',
                                    auto_offset_reset='earliest',enable_auto_commit =False,
                                    api_version=(2, 8, 1),consumer_timeout_ms=1000)

    #Until kafka_consumer has session, below loop will be running in infinitely
    for message in kafka_consumer:
        value: DataAndSchema = message.value
        # which are NamedTuples with a `data` and `schema` property
        value.data == value[0]
        value.schema == value[1]
        data, schema = value
        
        print("The value is : {}".format(data))
        print("The key is : {}".format(message.key))
        print("The topic is : {}".format(message.topic))
        print("The partition is : {}".format(message.partition))
        print("The offset is : {}".format(message.offset))
        print("The timestamp is : {}".format(message.timestamp))
        # Uncommenting this line added offset. This prevent displaying same message on second run
        # tp=TopicPartition(message.topic,message.partition)
        # om = OffsetAndMetadata(message.offset+1, message.timestamp)
        # kafka_consumer.commit({tp:om})
        print('*' * 100)
except Exception as e:
    raise ValueError(e)
