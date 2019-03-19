# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import logging

logger = logging.getLogger('Shenzhen')

class ShenzhenPipeline(object):
	def __init__(self):
		self.filename = open("code.json", "a")

	def process_item(self, item, spider):
		text = json.dumps(dict(item), ensure_ascii = False) + ",\n"
		logger.info(text);
		self.filename.write(text);
		return item

	def close_spider(self, spider):
		self.filename.close()