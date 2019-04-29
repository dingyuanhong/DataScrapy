# -*- coding: utf-8 -*-

import redis
from scrapy.conf import settings
from pymongo import MongoClient
import logging
from scrapy.exceptions import DropItem
# import json
# import simdjson
import ujson as json
# import simdjson as json

from .Core.MQQueue import MQSubscriber
from pipelines import StockDBPipeline

import logging
logger = logging.getLogger('Shenzhen')


config = {
	'exchange':{
		'key':'Shenzhen',
		'mode':'direct', #fanout direct topics
		'durable':True,
	},
	'route':'data',
	'queue':{
		'key':'data',
		'durable':True,
	}
}
args = {
	'HOST':'127.0.0.1',
	'PORT':'5672',
	'VHOST':'/',
	'credentials':{
		'name':'admin',
		'passwd':'admin',
	}
}

pipeline = StockDBPipeline()

def done(data):
	pipeline.process_item(json.dumps(data))

sub = MQSubscriber(config,args)
sub.wait(done);
