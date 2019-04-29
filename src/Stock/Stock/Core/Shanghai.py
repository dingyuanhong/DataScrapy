# -*- coding: utf-8 -*-

import random
from string import Template
# import json
# import simdjson
import ujson as json
# import simdjson as json
from bs4 import BeautifulSoup
import datetime
import os
import copy
from .Cache import Cache
import logging
logger = logging.getLogger('Shanghai')

class Core:
	@staticmethod
	def getMeta(core,meta):
		return {**meta,**{
			'scrapy:type':core['type'],
			'scrapy:domain':static_domain,
			'scrapy:random':core['random'],
			}
		}
	@staticmethod
	def merge(keys,values):
		#组合参数
		param = ''
		for key in keys:
			if param != '':
				param += '&'
			v = str(values[key])
			param += str(key) +'='+ urllib.parse.quote(v)
		return param;
	@staticmethod
	def subDone(core,meta,data,item):
		if 'subItem' in core:
			for t in core['subItem']:
				c = internal_findCore(t)
				if c == None:
					continue;
				if 'subGet' in c:
					for i in c['subGet'](c,meta,data,item):
						yield i;
		yield None;

#股票信息
class Stock:
	@staticmethod
	def get(core,param):
		pagenum = param['pagenum']
		pagesize = 25
		keys = ["callback","select","order","begin","end","_"]
		meta = {
			"callback":"",
			"select":"code,name,open,high,low,last,prev_close,chg_rate,volume,amount,tradephase,change,amp_rate",
			"order":"",
			"begin":pagenum,
			"end":pagenum+pagesize,
			"pageno":pagesize,
		};

		url = core['url'] + "?" + Core.merge(keys,meta);
		return {
			'url':url,
			'meta':Core.getMeta(core,param)
		}
	@staticmethod
	def parse(core,meta,body):
		data = body[1:][:-1].decode('gbk');
		# self.logger.info(data);
		js = json.loads(data) 
		for each in js['list']:
			item = {}
			item['date'] = js["date"];
			item['time'] = js["time"];
			#代码
			#简称
			#开盘
			#最高
			#最低
			#最新
			#前收
			#涨跌额
			#成交量
			#成交额
			#-------公式:T111
			#涨跌
			#振幅
			item['code'] = each[0]
			item['name'] = each[1]
			item['open'] = each[2]
			item['high'] = each[3]
			item['low'] = each[4]
			item['trade'] = each[5]
			item['settlement'] = each[6]
			item['pricechange'] = each[7]
			item['volume'] = each[8]
			item['amount'] = each[9]
			item['formula'] = each[10]
			item['changepercent'] = each[11]
			item['amplitude'] = each[12]
			yield item;

			yield Core.subDone(core,meta,each,item);
		yield core['end'](core,meta,js);

	@staticmethod
	def end(core,meta,data):
		if 'scrapy:loop' in meta:
			if not meta['scrapy:loop']:
				yield None;
				return;

		begin = int(data["begin"])
		end = int(data["end"])
		total = int(data["total"])

		pagesize = 25
		while end < total:
			end = end + pagesize
			meta = copy.deepcopy(meta);
			meta['pagenum'] = end
			yield meta;
		yield None

class Snap:
	@staticmethod
	def get(core,param):
		code = param['code']
		keys = ["callback","select","_"]
		values = {
			"callback":"",
			"select":"name,last,chg_rate,change,amount,volume,open,prev_close,ask,bid,high,low,tradephase",
		};
		url = self.snap + str(code) + "?" + Core.merge(keys,values);
		# self.logger.info(url);
		yield {
			'url':url,
			'meta':Core.getMeta(core,{'code':code})
		}
	@staticmethod
	def parse(core,meta,body):
		data = body[1:][:-1].decode('gbk');
		js = json.loads(data)

		item = {}
		item['date'] = js["date"];
		item['time'] = js["time"];
		item['code'] = js["code"];
		
		each = js['snap']
		#简称
		#最新价
		#涨幅
		#涨跌
		#成交额
		#成交量
		#开盘
		#昨收
		#卖盘5项
		#买盘5项
		#最高
		#最低
		#E111=股票
		item['name'] = each[0]
		item['trade'] = each[1]
		item['pricechange'] = each[2]
		item['changepercent'] = each[3]
		item['amount'] = each[4]
		item['volume'] = each[5]
		item['open'] = each[6]
		item['settlement'] = each[7]
		item['high'] = each[10]
		item['low'] = each[11]
		item['amplitude'] = each[12]

		item['sells'] = each[8]
		item['buys'] = each[9]
		yield item;

	@staticmethod
	def sub(core,meta,data,item):
		code = item['code']
		yield Core.getMeta(core,{
			'code':code,
		})
		pass

class Line:
	@staticmethod
	def get(core,param):
		code = param['code']
		# http://yunhq.sse.com.cn:32041/v1/sh1/line/000001?
		# callback=jQuery111202870652140273666_1553179236606
		# &begin=0
		# &end=-1
		# &select=time%2Cprice%2Cvolume
		# &_=1553179236612
		keys = ["callback","begin","end","select","_"]
		values = {
			"callback":"",
			"begin":0,
			"end":-1,
			"select":"time,price,volume",
		};
		url = self.line + str(code) + "?" + Core.merge(keys,values);
		# self.logger.info(url);
		return {
			'url':url,
			'meta':Core.getMeta(core,{'code':code})
		}
	@staticmethod
	def parse(core,meta,body):
		data = body[1:][:-1].decode('gbk');
		js = json.loads(data)

		#code: "600000"	 代码
		#highest: 11.59	 最高
		#lowest: 11.44	  最低
		#prev_close: 11.55	昨收
		#begin: 0
		#end: 241
		#total: 241   总数
		#date: 20190321
		#time: 154508
		item = {}
		item['date'] = js["date"];
		item['time'] = js["time"];
		item['code'] = js["code"];

		item['settlement'] = js['prev_close']
		item['high'] = js['highest']
		item['low'] = js['lowest']
		# js['line'];
		#时间
		#成交价
		#成交量
		item['line'] = js['line']
		yield item;
	@staticmethod
	def sub(core,meta,data,item):
		code = item['code']
		yield Core.getMeta(core,{
			'code':code,
		})
		pass

class KLine:
	@staticmethod
	def get(core,param):
		code = param['code']
		keys = ["callback","select","begin","end","_"]
		values = {
			"callback":"",
			"begin":0,
			"end":-1,
			"select":"date,open,high,low,close,volume",
		};
		url = self.kline + str(code) + "?" + MergeParam(keys,values);
		# self.logger.info(url);
		return {
			'url':url,
			'meta':Core.getMeta(core,{'code':code})
		}
	@staticmethod
	def parse(core,meta,body):
		data = body[1:][:-1].decode('gbk');
		js = json.loads(data)

		# code: "600000" #代码
		# begin: 4273 #开始索引
		# end: 4572	#结束索引
		# total: 4572 #总数

		item = {}
		item['type'] = 'kline';
		item['code'] = js['code'];

		# js["kline"];
		#时间
		#开盘
		#最高
		#最低
		#收盘
		#成交量

		for each in js["kline"]:
			item['date'] = each[0];
			item['open'] = each[1]
			item['high'] = each[2]
			item['low'] = each[3]
			item['trade'] = each[4]
			item['volume'] = each[5]
			yield copy.deepcopy(item);
	@staticmethod
	def sub(core,meta,data,item):
		code = item['code']
		yield Core.getMeta(core,{
			'code':code,
		})
		pass

cores = [
{
	'scrapy:request':{
		'priority':9,
	},
	'random':'_',  #"_":int(time.time()*1000)
	'type':'Stock',
	'url':'http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/equity',
	'get':Stock.get,
	'parse':Stock.parse,
	'end':Stock.end,
	'subItem':[
		'Snap',
		'Line',
		'KLine',
	]
},
{
	'scrapy:request':{
		'priority':8,
	},
	'random':'_',
	'type':'Snap',
	'url':'http://yunhq.sse.com.cn:32041/v1/sh1/snap/',
	'get':Snap.get,
	'parse':Snap.parse,
	'subGet':Snap.sub,
},
{
	'scrapy:request':{
		'priority':7,
	},
	'random':'_',
	'type':'Line',
	'url':'http://yunhq.sse.com.cn:32041/v1/sh1/line/',
	'get':Line.get,
	'parse':Line.parse,
	'subGet':Line.sub,
},
{
	'scrapy:request':{
		'priority':6,
	},
	'random':'_',
	'type':'KLine',
	'url':'http://yunhq.sse.com.cn:32041/v1/sh1/dayk/',
	'get':KLine.get,
	'parse':KLine.parse,
	'subGet':KLine.sub,
},
]


allowed_domains = ['sse.com.cn']
static_domain = 'Shanghai'

def internal_findCore(type_):
	for core in cores:
		if type_ == core['type']:
			return core;
	return None;