# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import logging
from pymongo import MongoClient
import redis

logger = logging.getLogger('Shanghai')

class ShanghaiPipeline(object):
	def __init__(self):
		self.conn = MongoClient('127.0.0.1', 27017)
		self.db = self.conn.Shanghai
		self.code = self.db.code  #代码
		self.snap = self.db.snap  #分钟
		self.line = self.db.line  #线
		self.kline = self.db.kline  #线

	def process_item(self, item, spider):
		if "type" not in item:
			return item;
		if item["type"] == "code":
			self.code.update({'code':item['code'],'date':item['date'],'time': item['time']},{'$set':item},True)
		elif item["type"] == "snap":
			self.snap.update({'code':item['code'],'date':item['date'],'time': item['time']},{'$set':item},True)
		elif item["type"] == "line":
			self.line.update({'code':item['code'],'date':item['date'],'time': item['time']},{'$set':item},True)
		elif item["type"] == "kline":
			self.kline.update({'code':item['code'],'date':item['date']},{'$set':item},True)
		return item
