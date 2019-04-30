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
logger = logging.getLogger('Shares')

class StockPipeline(object):
	def process_item(self, item, spider):
		if "scrapy:type" not in item:
			raise DropItem("Missing type in %s" % item)
		if "scrapy:domain" not in item:
			raise DropItem("Missing domain in %s" % item)

		return item

class StockRedisCheckPipeline(object):
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
			key = key + '_' + str(item['date']);
		if 'time' in item:
			key = key + '_' + str(item['time']);
		if 'full' in item:
			key = key + '_' + item['full'];
		if 'lastDate' in item:
			key = key + '_' + item['lastDate'];
		if 'marketTime' in item:
			key = key + '_' + item['marketTime'];
		if 'attachPath' in item:
			key = key + '_' + item['attachPath'];

		if self.cache.exists(key):
			logger.info("Already consume item:%s" % key);
			raise DropItem("Already consume item:%s" % item)

		# logger.info(item)
		# raise DropItem("Already consume item:%s" % item)

		return item

class StockRedisFlagPipeline(object):
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
			key = key + '_' + str(item['date']);
		if 'time' in item:
			key = key + '_' + str(item['time']);
		if 'full' in item:
			key = key + '_' + item['full'];
		if 'lastDate' in item:
			key = key + '_' + item['lastDate'];
		if 'marketTime' in item:
			key = key + '_' + item['marketTime'];
		if 'attachPath' in item:
			key = key + '_' + item['attachPath'];

		self.cache.set(key,'1');
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

		self.db = self.conn.Shanghai
		self.Stock = self.db.Stock  #代码
		self.Snap = self.db.Snap  #分钟
		self.Line = self.db.Line  #线
		self.KLine = self.db.KLine  #线

		self.upset = False

	def process_item(self, item, spider):
		type_ = item['scrapy:type']
		domain = item['scrapy:domain']

		del item['scrapy:type'];
		del item['scrapy:domain']
		if 'type' in item:
			type_ = item['type']
			del item['type'];

		if not self.upset:
			collection = getattr(self,type_,None)
			if collection != None:
				collection.insert_one(item)
			else:
				print("no found type:"+type_);
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
		elif domain == 'Shanghai':
			if type_ == "Stock":
				self.Stock.update({'code':item['code'],'date':item['date'],'time': item['time']},{'$set':item},self.upset)
			elif type_ == "Snap":
				self.Snap.update({'code':item['code'],'date':item['date'],'time': item['time']},{'$set':item},self.upset)
			elif type_ == "Line":
				self.Line.update({'code':item['code'],'date':item['date'],'time': item['time']},{'$set':item},self.upset)
			elif type_ == "KLine":
				self.KLine.update({'code':item['code'],'date':item['date']},{'$set':item},self.upset)
		return item