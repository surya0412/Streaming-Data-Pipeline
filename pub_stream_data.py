from faker import Faker
import json
import time
import random
import argparse
from foodProvider import FoodProviders
import sys
import datetime

from google.cloud import pubsub_v1

import argparse
import logging

from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.transforms.sql import SqlTransform
import apache_beam as beam
from apache_beam import transforms
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.dataframe.io import _DelimSplitter, read_csv
import os
import _thread

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/ApacheBeam/Stream/service-account-key.json"

# fake data config
MAX_NUMBER_ITEMS_IN_ORDER = 5
MAX_ADDONS = 3

# gcp config
project = "qwiklabs-gcp-00-54aaa138a689"
TOPIC = 'OrderTopic' 

# Creating a Faker instance and seeding to have the same results every time we execute the script
fake = Faker()
Faker.seed(100)

# Adding the newly created FoodProvider to the Faker instance
fake.add_provider(FoodProviders)

# creating function to generate the food Order
def make_order (ordercount = 12345):
    shop = fake.Restaurant_Name()
    # Each Order can have 1-5 foods in it
    food_list = []
    amount = 0
    for food in range(random.randint(1, MAX_NUMBER_ITEMS_IN_ORDER)):
        # Each Food can have 0-5 additional Addons to it
        AddOns_list = []
        for AddOns in range(random.randint(0, MAX_ADDONS)):
            AddOns_list.append(fake.AddOns())
            amount = amount + fake.AddOn_price()
        food_list.append({
            'FoodName': fake.food_item(),
            'AddOns': AddOns_list
        })
        amount = amount + fake.food_price()

    # message composition
    # phone_number = ''.join(e for e in fake.unique.phone_number() if e.isalnum())
    message = {
        'id': ordercount,
        'shop': shop,
        'name': fake.Indian_Names(),
        'phoneNumber': fake.unique.phone_number(), #f'+91 {phone_number[1:]}',
        'address': fake.address(),
        'food_item': food_list,
        'amount': str(amount) + ' INR' ,
        'publish_timestamp': str(datetime.datetime.now())
    }
    key = {'shop': shop}
    # print(key)
    # print("----------------"*10)

    return message, key


if __name__ == "__main__":

    parser=argparse.ArgumentParser()
    parser.add_argument("--project",dest='project_name', required=False, help='Provide your project name, check the readme file')
    parser.add_argument("--topic",dest='topic_name', required=False,help='Provide the topic name, check the readme file')
    args=parser.parse_args()
    publisher = pubsub_v1.PublisherClient()
    # project = args.project
    event_type = publisher.topic_path(project,TOPIC)
    try:
        publisher.get_topic(event_type)
    except:
        publisher.create_topic(event_type)
    
    i = 1
    # while True:
    while True:
        # message, key = _thread.start_new_thread(make_order(i), ())
        # message, key = _thread.start_new_thread(make_order(i), ())
        message, key = make_order(i)
        if i%10 == 0:
            time.sleep(125)
        data = str(json.dumps(message))
        data = data.encode('utf-8')
        publisher.publish("projects/{project}/topics/{topic_name}".format(project=project, topic_name=TOPIC), data=data)
        print(message)
        i +=1