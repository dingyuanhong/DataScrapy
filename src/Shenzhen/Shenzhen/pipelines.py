# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import logging
from pymongo import MongoClient
import redis

logger = logging.getLogger('Shenzhen')

class ShenzhenPipeline(object):
	def __init__(self):
		self.conn = MongoClient('127.0.0.1', 27017)
		self.db = self.conn.Shenzhen
		self.generalization = self.db.generalization  #公司概述
		self.company = self.db.company  #公司
		self.quotation = self.db.quotation	#个股详情
		self.index = self.db.index	#关键指标
		self.annIndex = self.db.annIndex	#公告
		self.market = self.db.market 		#市场
		self.historyDay = self.db.historyDay	#历史日线
		self.volume = self.db.volume	#成交量
		self.transaction = self.db.transaction	#成交详情

	def process_item(self, item, spider):
		if "type" not in item:
			return item;
		if item["type"] == "generalization":
			self.generalization.update({'code':item['code'] },{'$set':item},True)
		elif item['type'] == 'quotation':
			self.quotation.update({'date':item['date'],'code':item['code'] },{'$set':item},True)
		elif item["type"] == "company":
			self.company.update({'full':item['full'] },{'$set':item},True)
		elif item['type'] == 'index':
			self.index.update({'code':item['code'],'lastDate':item['lastDate'] },{'$set':item},True)
		elif item['type'] == 'annIndex':
			self.annIndex.update({'code':item['code'],'attachPath':item['attachPath'] },{'$set':item},True)
		elif item['type'] == 'market':
			self.market.update({'code':item['code'],'marketTime':item['marketTime'] },{'$set':item},True)
		elif item['type'] == 'historyDay':
			self.historyDay.update({'code':item['code'],'date':item['date'] },{'$set':item},True)
		elif item['type'] == 'volume':
			self.volume.update({'code':item['code'],'date':item['date'] },{'$set':item},True)
		elif item['type'] == 'transaction':
			self.transaction.update({'code':item['code'],'date':item['date'] },{'$set':item},True)
		return item

	def close_spider(self, spider):
		pass