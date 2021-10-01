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
from io import StringIO
# from google.cloud import 
from google.cloud import pubsub_v1

import argparse
import logging

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
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/ApacheBeam/Stream/service-account-key.json"
project = "qwiklabs-gcp-00-446df5a53b8a"
# json_data = StringIO("""[{
#     "id": "1",
#     "shop": "Salma",
#     "name": " sukesh",
#     "phoneNumber": "vv",
#     "address": "0353 Combs Ford Ap"
# }]""")

# print(json_data)

# ndjson = [json.dumps(record) for record in json.load(json_data)]

# print('\n'.join(ndjson))

# print(json.dumps(json_data, indent=4))

class transform(beam.DoFn):
  """Parse each line of input text into words."""

  def process(self, element):
    
    print(element)

    return element

class JsonCoder(object):
    """A JSON coder interpreting each line as a JSON string."""
    def encode(self, x):
        return json.dumps(x)

    def decode(self, x):
        return json.loads(x)

class decodemessage(beam.DoFn):
  """Parse each line of input text into words."""

  def process(self, element):
    print(element)
    data= json.loads(element)
    dictdata = ast.literal_eval(str(data))
    dictdata ['phoneNumber'] = re.sub("[^0-9]", "", dictdata ['phoneNumber'])
    ct = datetime.datetime.now()
    dictdata ['timestamp'] = str(ct)
    return [dictdata]


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the our pipeline."""
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

  def callback(message):
      print(message.data)
      message.ack() 
  def process(message):
      phoneno = message['phoneNumber']
      phoneno = ''.join(e for e in phoneno if e.isalnum())
      message['phoneNumber'] = phoneno
      return message

  with beam.Pipeline(options=pipeline_options) as p:
    # subscriber = pubsub_v1.SubscriberClient()
    # topic_path= "projects/{project}/subscritions/{topic_name}".format(project=project, topic_name=known_args.topic_name)
    # sub_path= "projects/{project}/subscritions/{sub_name}".format(project=project, sub_name=known_args.sub_name)

    lines = (p | 'some' >> beam.io.ReadFromText("C:/ApacheBeam/Stream/something.json")
            #    | 'Print1' >> beam.Map(json.loads)
               | 'Decode the Message' >> beam.ParDo(decodemessage())
               | 'Print' >> beam.Map(print))



if __name__ == '__main__':
  run()

