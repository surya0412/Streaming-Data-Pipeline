from apache_beam.io import avroio
from apache_beam.io.avroio import _AvroUtils
from apache_beam.transforms.combiners import Count
from apache_beam.transforms.core import Map
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
from apache_beam import window
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToAvro
from apache_beam.io.gcp.pubsub import pubsub   
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.dataframe.io import _DelimSplitter, read_csv
from apache_beam.io.gcp.bigtableio import WriteToBigTable


import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/ApacheBeam/Streaming-Data-Pipeline/config/service-account-key.json"
project = "qwiklabs-gcp-03-2ba6cfae58ed"


class get_req_col(beam.DoFn):
  """Parse each line of input text into words."""

  def process(self, element):  
    
    return [(element['shop'],element['id'])]

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
    # print(type(dictdata))
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
  parser.add_argument(
      '--output_avro_file',
      dest='output_avro_file',
      default='C:/ApacheBeam/Streaming-Data-Pipeline/config/output.avro',
      help='Subscriber name for that topic')
  known_args, pipeline_args = parser.parse_known_args(argv)
  
  pipeline_options = PipelineOptions(pipeline_args,streaming=True, save_main_session=True)


  def sumall(message):
    shop_name, id = message
    return (shop_name, len(id)) 

  def dict_format(message):
    shop,id = message
    return {'shop':shop, 'count':id}

  with beam.Pipeline(options=pipeline_options) as p:

    subscriber = pubsub_v1.SubscriberClient()
    topic_path= "projects/{project}/subscritions/{topic_name}".format(project=project, topic_name=known_args.topic_name)
    sub_path= "projects/{project}/subscritions/{sub_name}".format(project=project, sub_name=known_args.sub_name)

    pubsub_messages = (p | 'Read' >> beam.io.ReadFromPubSub(topic="projects/{project}/topics/OrderTopic".format(project=project))
                         | 'Decode the Pub/Sub Message' >> (beam.ParDo(decode()))
                         | 'perform transformations' >> (beam.ParDo(transform())))
    print(str(datetime.datetime.now()))

    avro_schema = {
    "name": "bqtable",
    "type": "record",
    "fields": [
        {"name": "shop", "type": "string"},
        {"name": "count", "type": ["null", "int"]},
    ]
}
 
    running_amount_averages = (
         pubsub_messages 
        #  | 'print time' >> beam.Map(print(str(datetime.datetime.now())))
                         | 'window' >> beam.WindowInto(window.SlidingWindows(60, 30),accumulation_mode="ACCUMULATING")
                         | 'filter column' >> (beam.ParDo(get_req_col()))
                         | 'Calculate Average Price and restaurant' >> (beam.GroupByKey())
                         | 'get the count' >> (beam.Map(sumall))
                        #  | 'print time' >> beam.Map(lambda x:str(datetime.datetime.now()))
                        # | 'print the count' >> (beam.Map(print))
                        #  | 'add timestamp' >> (beam.Map(lambda x:str(datetime.datetime.now())))
                        #  | 'print' >> beam.Map()))
                         | 'dict format for avro' >> (beam.Map(dict_format))
                         | 'Write to Avro' >> WriteToAvro(known_args.output_avro_file,schema=avro_schema, file_name_suffix='.avro'))

    # print(str(datetime.datetime.now()))

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

    # print(table_schema)
    pubsub_messages | beam.Map(print)

    pubsub_messages | beam.io.WriteToBigQuery(
    table = table_id,
    dataset=dataset_id,
    project=project,
    schema=table_schema,
    with_auto_sharding=True,
    method='STREAMING_INSERTS',
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    # sliding_window_table_schema = bigquery.TableSchema()
    #   # shop
    # shop_schema = bigquery.TableFieldSchema()
    # shop_schema.name = 'shop'
    # shop_schema.type = 'STRING'
    # shop_schema.mode = 'NULLABLE'
    # sliding_window_table_schema.fields.append(shop_schema)

    #   # name
    # count_schema = bigquery.TableFieldSchema()
    # count_schema.name = 'orders_in_last_one_hour'
    # count_schema.type = 'STRING'
    # count_schema.mode = 'NULLABLE'
    # sliding_window_table_schema.fields.append(count_schema)


    # table_id = 'sliding_window_table'
    # pubsub_messages | beam.io.WriteToBigQuery(
    # table = table_id,
    # dataset=dataset_id,
    # project=project,
    # schema=sliding_window_table_schema,
    # with_auto_sharding=True,
    # method='STREAMING_INSERTS',
    # write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    # create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)



    # pubsub_messages | WriteToBigTable(
    #   project_id = project,
    #   instance_id = '',
    #   table_id = '')

               
if __name__ == '__main__':
  run()

