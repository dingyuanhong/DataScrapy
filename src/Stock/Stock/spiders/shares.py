# -*- coding: utf-8 -*-
import scrapy
from scrapy.conf import settings
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
from scrapy_redis.spiders import RedisSpider

import sys
sys.path.append("..")
from util.logger import getLogger
import Stock.Core.Shenzhen as Shenzhen

class SharesSpider(scrapy.Spider):
	name = 'shares'
	allowed_domains = ['szse.cn']

	logger = getLogger('Shenzhen');

	keys = ['tab1','tab2','tab3','tab4'];

	config = [{
		'scrapy:domain':Shenzhen.static_domain,
		'scrapy:type':'Report',
		'key':keys[0],
		'pageno':1
	}]

	def start_requests(self):
		for each in self.config:
			core = Shenzhen.findCore(each)
			for req in core['get'](core,each):
				yield self.getRequest(core,req)

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

	def getRequest(self,core,req):
		key = 'scrapy:request' 
		if key in core and len(core[key].keys()) > 0:
			if 'method' not in req:
				return scrapy.Request(req['url'],meta=req['meta'],**core[key],callback= self.parse)
			else:
				return scrapy.FormRequest(**req,**core[key],callback = self.parse);
		else:
			if 'method' not in req:
				return scrapy.Request(req['url'],meta=req['meta'],callback= self.parse)
			else:
				return scrapy.FormRequest(**req,callback = self.parse);

	def parse(self, response):
		meta = copy.deepcopy(response.meta);
		del meta['download_slot'];  #域
		del meta['download_latency']; #延迟
		del meta['download_timeout']; #超时
		del meta['depth'];			#深度

		self.logger.info(response.url 
			+ " 延迟:" +  str(response.meta['download_latency']) 
			+ " 超时:" + str(response.meta['download_timeout']));
		
		if settings.get('IGNOREREQUEST'):
			return;

		body = response.body.decode('utf-8')
		# self.logger.info(response.url);
		# self.logger.info(meta);
		# self.logger.info(body);

		core = Shenzhen.findCore(meta)
		result = core['parse'](core,meta,body);

		for data in result:
			if data == None:
				continue;
			# self.logger.info(data)
			if isinstance(data,GeneratorType):
				for each in self.Generator3Layer(data):
					if each == None:
						continue;
					if not isinstance(each,dict):
						self.logger.error("type error:" + each);
						continue;

					c = Shenzhen.findCore(each)
					for req in c['get'](c,each):
						yield self.getRequest(c,req)
			else:
				if data != None:
					data['scrapy:type'] = core['type'];
					data['scrapy:domain'] = Shenzhen.static_domain;
					yield data;
