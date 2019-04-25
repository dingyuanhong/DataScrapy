# -*- coding: utf-8 -*-
import scrapy
import random
from string import Template
import json
from bs4 import BeautifulSoup
import datetime
from types import GeneratorType
from collections import Iterable, Iterator
import redis
import os
import copy
import sys
sys.path.append("..")
from util.logger import getLogger
from scrapy_redis.spiders import RedisSpider
from ..Core.Shenzhen import findCore
from scrapy_redis.spiders import RedisSpider

class SharesSpider(scrapy.Spider):
	name = 'shares'
	allowed_domains = ['szse.cn']

	logger = getLogger('Shenzhen');

	keys = ['tab1','tab2','tab3','tab4'];

	config = [{
		'scrapy:type':'Report',
		'key':keys[0],
		'pageno':1
	}]

	def start_requests(self):
		for each in self.config:
			core = findCore(each['scrapy:type'])
			for request in core['get'](core,each):
				if 'method' not in request:
					yield scrapy.Request(request['url'],meta=request['meta'],callback= self.parse)
				else:
					yield scrapy.FormRequest(**request,callback = self.parse);

	def Generator3Layer(self,data):
		if isinstance(data,GeneratorType):
			for a in data:
				if isinstance(a,GeneratorType):
					for b in a:
						if isinstance(b,GeneratorType):
							for c in b:
								if isinstance(c,GeneratorType):
									for d in c:
										yield d;
								else:
									yield c;
						else:
							yield b;
				else:
					yield a;
		else:
			yield data;

	def parse(self, response):
		meta = copy.deepcopy(response.meta);
		del meta['download_slot'];  #域
		del meta['download_latency']; #延迟
		del meta['download_timeout']; #超时
		del meta['depth'];			#深度

		self.logger.info(response.url 
			+ " 延迟:" +  str(response.meta['download_latency']) 
			+ " 超时:" + str(response.meta['download_timeout']));

		body = response.body.decode('utf-8')
		# self.logger.info(response.url);
		# self.logger.info(meta);
		# self.logger.info(body);
		# return;
		core = findCore(meta['scrapy:type'])
		result = core['parse'](core,meta,body);
		for data in result:
			if data == None:
				continue;
			if isinstance(data,GeneratorType):
				for each in self.Generator3Layer(data):
					if each == None:
						continue;
					if not isinstance(each,dict):
						self.logger.error("type error:" + each);
						continue;
					req = each;
					if 'method' not in req:
						yield scrapy.Request(req['url'],meta=req['meta'],callback= self.parse)
					else:
						yield scrapy.FormRequest(**req,callback = self.parse);
			else:
				if data != None:
					yield data;
