# Imports
import boto3
from botocore.exceptions import ClientError
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from aws_schema_registry import SchemaRegistryClient
from aws_schema_registry.avro import AvroSchema
from aws_schema_registry.adapter.kafka import KafkaSerializer

# Variables
# --------------------------------------------------------------------------
REGISTRY_NAME = 'GlueSchemaRegistry'
BOOTSTRAP_SERVER = 'b-3.mskworkshopcluster.6pux1a.c3.kafka.us-east-2.amazonaws.com'
SCHEMA_S3_PATH = 's3://aws-glue-assets-079448565720-us-east-2/kafkaResources/customer.avsc'
KAFKA_TOPIC = 'gsr-kafka-topic' 
# --------------------------------------------------------------------------
FILE_NAME = '/tmp/Schema.avsc'

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')


# Function to get S3 bucket name and prefix
# or follow https://stackoverflow.com/questions/42641315/s3-urls-get-bucket-name-and-path
def get_bucket_detail(s3_path: str):
    bucket_name = prefix = ""
    if s3_path:
        rmv_s3_pro = s3_path.replace('s3://', '')
        s3_dict = rmv_s3_pro.split('/')
        if len(s3_dict) > 0 and s3_dict[0]:
            bucket_name = s3_dict[0]
            s3_dict.remove(bucket_name)
            prefix = "/".join(map(str, s3_dict))
        return bucket_name, prefix


# Function to get or create Glue Schema Registry name
def getOrCreateRegistry(glue_client, registry_name):
    if glue_client and registry_name:
        try:
            response_get_registry = glue_client.get_registry(RegistryId={'RegistryName': registry_name})
            print(f'Glue Schema {registry_name} already exist !!')
            return response_get_registry['RegistryName']
        except ClientError as e:
            try:
                if e.response['Error']['Code'] == 'EntityNotFoundException':
                    response_create_registry = glue_client.create_registry(RegistryName=registry_name)
                    print(f'Registry {registry_name} created successfully in AWS Glue.')
                    return response_create_registry['RegistryName']
            except Exception as e:
                raise ValueError(e)

#Function to check or create Topic in Apache Kafka
def checkOrCreateTopic(broker,topic):
    topic_list = []
    client_group_id = 'aws-glue-schema-registry'
    if broker and topic:
        try:
            topic_dict = KafkaConsumer(group_id=client_group_id, bootstrap_servers=broker).topics()
            if topic in topic_dict:
                print(f'Topic {topic} already exist in Apache Kafka')
                return topic
            else:
                kafka_admin_client = KafkaAdminClient(bootstrap_servers=broker,client_id=client_group_id,
                                                      api_version=(0, 10, 1))
                topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=3))
                kafka_admin_client.create_topics(new_topics=topic_list, validate_only=False)
                print(f'Topic {topic} created successfully in Apache Kafka')
                return topic
        except Exception as e:
            raise ValueError(e)

# Getting Glue Schema Registry client and creating serializer for Kafka Producer
gsr_client = SchemaRegistryClient(glue_client, registry_name=getOrCreateRegistry(glue_client, REGISTRY_NAME))
gsr_serializer = KafkaSerializer(gsr_client)
print("Glue Schema Registry and Serializer created")

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=gsr_serializer, api_version=(0, 10, 1))
print("Kafka Producer created with Glue Schema Registry ")

# Downloading schema file from the S3 location to glue tmp folder
try:
    BUCKET_NAME, FILE_PATH = get_bucket_detail(SCHEMA_S3_PATH)
    s3_client.download_file(BUCKET_NAME, FILE_PATH, FILE_NAME)
except Exception as e:
    raise ValueError("Downloading Schema from the S3 bucket got failed. Please check the S3 path.{}".format(e))
with open(FILE_NAME, 'r') as schema_file:
    schema = AvroSchema(schema_file.read())

# Data to send to Kafka
data = {

    'first_name': 'Amazon',
    'last_name': 'Web Service',
    'age':24,
    'height':242.5,
    'weight':69.5,
    'automated_email':'xyz@gmaik.com'
    # 'full_name': 'AWS Glue Schema Registry two'

}

print("Glue start sending topic to Producer")
record_metadata = producer.send(checkOrCreateTopic(BOOTSTRAP_SERVER,KAFKA_TOPIC), value=(data, schema)).get(timeout=10)
print(f'Glue job send data to topic {record_metadata.topic}')
print(f'Kafka Partition {record_metadata.partition}')
print(f'Kafka Topic offset {record_metadata.offset}')
