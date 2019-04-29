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
import importlib
import logging
import sys
sys.path.append("..")
from util.logger import getLogger
from scrapy_redis.spiders import RedisSpider

class RulesControl:
	rules = copy.deepcopy(settings.get('RULES'))
	def get_domains(self,):
		allowed_domains = []
		for i in range(0,len(self.rules)):
			rule_ = self.rules[i];
			if isinstance (rule_,str):
				rule_ = importlib.import_module(rule_)
				self.rules[i] = rule_;
			if rule_ == None:
				continue;
			allowed_domains.extend(rule_.allowed_domains);
		return allowed_domains;

	def findCore(self,rule,meta):
		if not meta['scrapy:domain'] == rule.static_domain:
			return None;
		return rule.internal_findCore(meta['scrapy:type'])

	def findRule(self,meta):
		for rule in self.rules:
			if rule == None:
				continue;
			core = self.findCore(rule,meta)
			if core != None:
				return rule , core
		return None,None

class SharesSpider(scrapy.Spider):
	name = 'shares'
	rules = RulesControl()
	allowed_domains = rules.get_domains()

	logger = getLogger('Shares');

	keys = ['tab1','tab2','tab3','tab4'];

	config = [
	{
		'scrapy:domain':'Shenzhen',
		'scrapy:type':'Report',
		'key':keys[0],
		'pageno':1
	},
	{
		'scrapy:domain':'Shenzhen',
		'scrapy:type':'Report',
		'key':keys[1],
		'pageno':1
	},
	{
		'scrapy:domain':'Shenzhen',
		'scrapy:type':'Report',
		'key':keys[2],
		'pageno':1
	},
	{
		'scrapy:domain':'Shenzhen',
		'scrapy:type':'Report',
		'key':keys[3],
		'pageno':1
	},
	{
		'scrapy:domain':'Shanghai',
		'scrapy:type':'Stock',
		'pageno':0
	}
	]

	def start_requests(self):
		for each in self.config:
			rule,core = self.rules.findRule(each)
			if rule == None:
				self.logger.error("find rule empty:",each);
				continue;
			data = core['get'](core,each)
			if isinstance(data,GeneratorType):
				for req in data:
					yield self.getRequest(core,req)
			else:
				yield self.getRequest(core,data)

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

		# self.logger.info(response.url 
		# 	+ " 延迟:" +  str(response.meta['download_latency']) 
		# 	+ " 超时:" + str(response.meta['download_timeout']));
		
		if settings.get('IGNOREREQUEST'):
			return;

		rule,core = self.rules.findRule(meta)
		if rule == None:
			self.logger.error("find rule empty:",meta);
			return;
		if rule.language != '':
			body = response.body.decode(rule.language)
		else:
			body = response.body
		# self.logger.info(response.url);
		# self.logger.info(meta);
		# self.logger.info(body);

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
					# self.logger.info(each)
					c = self.rules.findCore(rule,each)
					for req in c['get'](c,each):
						yield self.getRequest(c,req)
			else:
				if data != None:
					data['scrapy:type'] = core['type'];
					data['scrapy:domain'] = rule.static_domain;
					yield data;
