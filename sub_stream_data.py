from faker import Faker
import json
import time
import random
import argparse
from foodProvider import FoodProviders
import sys
import ast
import re
import datetime;
  
# from google.cloud import 
from google.cloud import pubsub_v1

import argparse
import logging

from google.cloud import bigquery as bq
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.transforms.sql import SqlTransform
import apache_beam as beam
from apache_beam import transforms
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.gcp.pubsub import pubsub   
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.dataframe.io import _DelimSplitter, read_csv
from apache_beam.io.gcp.bigtableio import WriteToBigTable


import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/ApacheBeam/Stream/service-account-key.json"
project = "qwiklabs-gcp-00-54aaa138a689"


# class transform(beam.DoFn):
#   """Parse each line of input text into words."""

#   def process(self, element):  
#     print(element)
#     return element

class decodemessage(beam.DoFn):
  """Parse each line of input text into words."""

  def process(self, element):
    data= json.loads(element.decode('utf-8'))
    dictdata = ast.literal_eval(str(data))
    dictdata['id'] = str(dictdata['id'])
    dictdata ['phoneNumber'] = re.sub("[^0-9]", "", dictdata ['phoneNumber'])
    dictdata ['amount'] = re.sub("[^0-9]", "", dictdata ['amount'])
    # ct = datetime.datetime.now()
    # dictdata ['timestamp'] = str(ct)
    yield dictdata
    # yield {'id':dictdata['id'], 'name':dictdata['name'], 'shop':dictdata['shop'], 'phoneNumber':dictdata['phoneNumber'], 'address':dictdata['address'], 'timestamp':dictdata['timestamp']}
    # yield {'id':dictdata['id'], 'name':dictdata['name']}

class decode(beam.DoFn):
  """Parse each line of input text into words."""

  def process(self, element):
    element = element.decode('utf-8')
    data= json.loads(element)
    yield data

class transform(beam.DoFn):
  """Parse each line of input text into words."""

  def process(self, element):
    dictdata = element
    print(type(dictdata))
    # dictdata = ast.literal_eval(str(element))
    dictdata ['phoneNumber'] = re.sub("[^0-9]", "", dictdata ['phoneNumber'])
    dictdata ['amount'] = re.sub("[^0-9]", "", dictdata ['amount'])
    
    yield dictdata



def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the our pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--topic_name',
      dest='topic_name',
      default='OrderTopic',
      help='Topic Name')
  parser.add_argument(
      '--sub_name',
      dest='sub_name',
      default='sub-OrderTopic',
      help='Subscriber name for that topic')
  known_args, pipeline_args = parser.parse_known_args(argv)
  
  pipeline_options = PipelineOptions(pipeline_args,streaming=True, save_main_session=True)


  def callback(message):
      print(message.data)
      message.ack() 
  def process(message):
      phoneno = message['phoneNumber']
      phoneno = ''.join(e for e in phoneno if e.isalnum())
      message['phoneNumber'] = phoneno
      return message
  with beam.Pipeline(options=pipeline_options) as p:

    subscriber = pubsub_v1.SubscriberClient()
    topic_path= "projects/{project}/subscritions/{topic_name}".format(project=project, topic_name=known_args.topic_name)
    sub_path= "projects/{project}/subscritions/{sub_name}".format(project=project, sub_name=known_args.sub_name)

    pubsub_messages = (p | 'Read' >> beam.io.ReadFromPubSub(topic="projects/{project}/topics/OrderTopic".format(project=project))
               | 'Decode the Pub/Sub Message' >> (beam.ParDo(decode()))
               | 'perform transformations' >> (beam.ParDo(transform())))
              #  | 'Print' >> (beam.Map(print)))
    # print("something")
              #  | 'JSON row to dict' >> beam.ParDo(transform())
              #  | 'Filter Data' >> (beam.Map(process)))
    # transforming = (lines | (beam.Map(process)))
                        #   | 'Group' >> (beam.GroupByKey())
                        #   | 'sum' >> (beam.Map(sum_all))
                        #   | 'Largest 5 values' >> beam.combiners.Top.Largest(5))
    # (transforming 
    # | 'Write in Format' >> beam.MapTuple(write_format)
    #              |WriteToText(known_args.output))

    # CREATE TABLE `qwiklabs-gcp-03-b60faab5f29e.mydataset.mytable`
    # (
    #   id STRING,
    #   shop STRING,
    #   name STRING,
    #   phoneNumber STRING,
    #   address STRING,
    #   food_item ARRAY<STRUCT<FoodName STRING, AddOns ARRAY<STRING>>>,
    #   timestamp STRING
    # );

    # Define Table Schema 
    table_schema = bigquery.TableSchema()

      # id
    id_schema = bigquery.TableFieldSchema()
    id_schema.name = 'id'
    id_schema.type = 'INTEGER'
    id_schema.mode = 'NULLABLE'
    table_schema.fields.append(id_schema)

      # name
    name_schema = bigquery.TableFieldSchema()
    name_schema.name = 'name'
    name_schema.type = 'STRING'
    name_schema.mode = 'NULLABLE'
    table_schema.fields.append(name_schema)

      # shop
    shop_schema = bigquery.TableFieldSchema()
    shop_schema.name = 'shop'
    shop_schema.type = 'STRING'
    shop_schema.mode = 'NULLABLE'
    table_schema.fields.append(shop_schema)

      # phoneNumber
    phoneNumber_schema = bigquery.TableFieldSchema()
    phoneNumber_schema.name = 'phoneNumber'
    phoneNumber_schema.type = 'INTEGER'
    phoneNumber_schema.mode = 'NULLABLE'
    table_schema.fields.append(phoneNumber_schema)

      # address
    address_schema = bigquery.TableFieldSchema()
    address_schema.name = 'address'
    address_schema.type = 'STRING'
    address_schema.mode = 'NULLABLE'
    table_schema.fields.append(address_schema)

      # food_item
    food_item_schema = bigquery.TableFieldSchema()
    food_item_schema.name = 'food_item'
    food_item_schema.type = 'RECORD'
    food_item_schema.mode = 'REPEATED'

      # FoodName
    FoodName_schema = bigquery.TableFieldSchema()
    FoodName_schema.name = 'FoodName'
    FoodName_schema.type = 'STRING'
    FoodName_schema.mode = 'NULLABLE'
    food_item_schema.fields.append(FoodName_schema)

      # AddOns
    AddOns_schema = bigquery.TableFieldSchema()
    AddOns_schema.name = 'AddOns'
    AddOns_schema.type = 'STRING'
    AddOns_schema.mode = 'REPEATED'
    food_item_schema.fields.append(AddOns_schema)

    table_schema.fields.append(food_item_schema)
    
      # amount
    amount_schema = bigquery.TableFieldSchema()
    amount_schema.name = 'amount'
    amount_schema.type = 'INTEGER'
    amount_schema.mode = 'NULLABLE'
    table_schema.fields.append(amount_schema)

      # publish_timestamp
    timestamp_schema = bigquery.TableFieldSchema()
    timestamp_schema.name = 'publish_timestamp'
    timestamp_schema.type = 'TIMESTAMP'
    timestamp_schema.mode = 'NULLABLE'
    table_schema.fields.append(timestamp_schema)

    table_id = 'mytable'
    dataset_id = 'mydataset'

    print(table_schema)
    pubsub_messages | beam.Map(print)

    # pubsub_messages | beam.io.WriteToBigQuery(
    # table = table_id,
    # dataset=dataset_id,
    # project=project,
    # schema=table_schema,
    # # with_auto_sharding=True,
    # method='STREAMING_INSERTS',
    # write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    # create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    pubsub_messages | WriteToBigTable(
      project_id = project,
      instance_id = '',
      table_id = '')

               
if __name__ == '__main__':
  run()

