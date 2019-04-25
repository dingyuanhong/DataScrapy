# -*- coding: utf-8 -*-
import scrapy
import random
from string import Template
import json
from bs4 import BeautifulSoup
import datetime
from collections import Iterable
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
				self.logger.info(request)
				if 'method' not in request:
					yield scrapy.Request(request['url'],meta=request['meta'],callback= self.parse)
				else:
					yield scrapy.FormRequest(**request,callback = self.parse);

	def parse(self, response):
		meta = response.meta;
		del meta['download_slot'];  #域
		del meta['download_latency']; #延迟
		del meta['download_timeout']; #超时
		del meta['depth'];			#深度

		body = response.body.decode('utf-8')
		# self.logger.info(response.url);
		# self.logger.info(response.meta);
		# self.logger.info(body);
		# return;
		core = findCore(meta['type'])
		result = core['parse'](core,meta,body);
		for data in result:
			if data == None:
				continue;
			if isinstance(data,Iterable):
				for request in data:
					if request == None:
						continue;
					self.logger.info(request)
					if 'method' not in request:
						yield scrapy.Request(request['url'],meta=request['meta'],callback= self.parse)
					else:
						yield scrapy.FormRequest(**request,callback = self.parse);
			else:
				yield data;
