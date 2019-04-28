# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import redis
from scrapy.conf import settings
from pymongo import MongoClient
import logging
from scrapy.exceptions import DropItem
# import json
# import simdjson
import ujson as json
# import simdjson as json

from .Core.MQQueue import MQPublish

import logging
logger = logging.getLogger('Shenzhen')

class StockPipeline(object):
	def process_item(self, item, spider):
		if "scrapy:type" not in item:
			raise DropItem("Missing type in %s" % item)
		if "scrapy:domain" not in item:
			raise DropItem("Missing domain in %s" % item)

		return item

class StockRedisPipeline(object):
	def __init__(self):
		HOST = settings.get('REDIS_HOST')
		PORT = settings.get('REDIS_PORT')
		pool = redis.ConnectionPool(host=HOST, port=PORT)
		self.cache = redis.Redis(connection_pool=pool)

	def process_item(self, item, spider):
		type_ = item['scrapy:type']
		domain = item['scrapy:domain']

		key = domain + "_" + type_;
		if 'code' in item:
			key = key + '_' + item['code'];
		if 'date' in item:
			key = key + '_' + item['date'];
		if 'full' in item:
			key = key + '_' + item['full'];
		if 'lastDate' in item:
			key = key + '_' + item['lastDate'];
		if 'marketTime' in item:
			key = key + '_' + item['marketTime'];
		if 'attachPath' in item:
			key = key + '_' + item['attachPath'];

		if self.cache.exists(key):
			logger.info("Already item:" + key);
			raise DropItem("Already consume item:%s" % item)

		self.cache.set(key,'');
		# self.cache.set(key,json.dumps(item));

		return item

class StockMQPipeline(object):
	def __init__(self):
		config = {
			'exchange':{
				'key':'Shenzhen',
				'mode':'direct', #fanout direct topics
				'durable':True,
			},
			'route':'data'
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
		self.publish = MQPublish(config,args);

	def process_item(self, item, spider):

		self.publish.publish(json.dumps(item));

		# raise DropItem("Already consume item:%s" % item)
		
		return item

class StockDBPipeline(object):
	def __init__(self):
		self.conn = MongoClient('127.0.0.1', 27017)
		self.db = self.conn.Shenzhen
		self.Report = self.db.Report		#公司概述
		self.HistoryDay = self.db.HistoryDay	#历史日线  超慢
		self.Quotation = self.db.Quotation	#个股详情
		self.Company = self.db.Company		#公司
		self.Index = self.db.Index			#关键指标
		self.AnnIndex = self.db.AnnIndex	#公告
		self.AnnList = self.db.AnnList		#历史公告
		self.Market = self.db.Market		#市场
		self.Volume = self.db.Volume		#成交量
		self.Transaction = self.db.Transaction	#成交详情
		self.upset = False

	def process_item(self, item, spider):
		type_ = item['scrapy:type']
		domain = item['scrapy:domain']

		del item['scrapy:type'];
		del item['scrapy:domain']

		if domain == 'Shenzhen' and not self.upset:
			if type_ == "Report":
				self.Report.insert_one(item)
			elif type_ == 'HistoryDay':
				self.HistoryDay.insert_one(item)
			elif type_ == 'Quotation':
				self.Quotation.insert_one(item)
			elif type_ == "Company":
				self.Company.insert_one(item)
			elif type_ == 'Index':
				self.Index.insert_one(item)
			elif type_ == 'AnnIndex':
				self.AnnIndex.insert_one(item)
			elif type_ == 'Market':
				self.Market.insert_one(item)
			elif type_ == 'Volume':
				self.Volume.insert_one(item)
			elif type_ == 'transaction':
				self.transaction.insert_one(item)
			elif type_ == 'AnnList':
				self.AnnList.insert_one(item)
		elif domain == 'Shenzhen':
			if type_ == "Report":
				self.Report.update({'code':item['code'] },{'$set':item},self.upset)
			elif type_ == 'HistoryDay':
				self.HistoryDay.update({'date':item['date'],'code':item['code']},{'$set':item},self.upset)
			elif type_ == 'Quotation':
				self.Quotation.update({'date':item['date'],'code':item['code'] },{'$set':item},self.upset)
			elif type_ == "Company":
				self.Company.update({'full':item['full'] },{'$set':item},self.upset)
			elif type_ == 'Index':
				self.Index.update({'code':item['code'],'lastDate':item['lastDate'] },{'$set':item},self.upset)
			elif type_ == 'AnnIndex':
				self.AnnIndex.update({'code':item['code'],'attachPath':item['attachPath'] },{'$set':item},self.upset)
			elif type_ == 'Market':
				self.Market.update({'code':item['code'],'marketTime':item['marketTime'] },{'$set':item},self.upset)
			elif type_ == 'Volume':
				self.Volume.update({'date':item['date'],'code':item['code']},{'$set':item},self.upset)
			elif type_ == 'transaction':
				self.transaction.update({'date':item['date'],'code':item['code']},{'$set':item},self.upset)
			elif type_ == 'AnnList':
				self.AnnList.update({'attachPath':item['attachPath'],'code':item['code']},{'$set':item},self.upset)
		return item