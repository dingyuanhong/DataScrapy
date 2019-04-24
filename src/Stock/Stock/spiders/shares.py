# -*- coding: utf-8 -*-
import scrapy
import random
from string import Template
import json
from bs4 import BeautifulSoup
import datetime
from collections import Iterator
import redis
import os
import sys
sys.path.append("..")
from util.logger import getLogger
from scrapy_redis.spiders import RedisSpider
from ..Core.Shenzhen import findCore

class SharesSpider(scrapy.Spider):
	name = 'shares'
	allowed_domains = ['szse.cn']

	logger = getLogger('Shenzhen');

	keys = ['tab1','tab2','tab3','tab4'];

	config = [{
		'type':'Report',
		'key':keys[0],
		'pageno':1
	}]

	def start_requests(self):
		for each in self.config:
			core = findCore(each['type'])
			for request in core['get'](core,each):
				if 'method' not in request:
					yield scrapy.Request(request['url'],meta=request['meta'],callback= self.parse)
				else:
					yield scrapy.FormRequest(**request,callback = self.parse);

	def parse(self, response):
		meta = response.meta;
		self.logger.info(response.url);
		self.logger.info(meta);
		return;
		core = findCore(meta['type'])
		result = core['parse'](core,meta,response.body);
		for data in result:
			if data == None:
				continue;
			if isstance(data,Iterator):
				for request in data:
					if request == None:
						continue;
					if 'method' not in request:
						yield scrapy.Request(request['url'],meta=request['meta'],callback= self.parse)
					else:
						yield scrapy.FormRequest(**request,callback = self.parse);
			else:
				yield data;
