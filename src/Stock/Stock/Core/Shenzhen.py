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
logger = logging.getLogger('Shenzhen')


static_domain = 'Shenzhen'

class Core:
	@staticmethod
	def getMeta(core,meta):
		return {**meta,**{'scrapy:type':core['type'],'scrapy:domain':static_domain}}
	@staticmethod
	def get(core,param):
		meta = param;
		yield {
			'url':core['url'].substitute(meta),
			'meta':Core.getMeta(core,meta)
		}
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
class Report:
	@staticmethod
	def parse(core,meta,body):
		key = meta['key']
		js = json.loads(body)
		for data in js:
			if data['metadata']['tabkey'] == key:
				for each in data['data']:
					item = Report.parseItem(core,data,each);
					yield item;
					yield Core.subDone(core,meta,each,item);
				yield core['end'](core,meta,data);
		yield None
	@staticmethod
	def end(core,meta,data):
		if 'scrapy:loop' in meta:
			if not meta['scrapy:loop']:
				yield None;
				return;

		pageno = int(data['metadata']['pageno'])
		pagesize = int(data['metadata']['pagesize'])
		total = int(data['metadata']['recordcount'])
		while pageno * pagesize < total:
			pageno = pageno +1;
			meta = copy.deepcopy(meta)
			meta['scrapy:loop'] = False;
			meta['pageno'] = pageno;
			# yield core['get'](core,meta)
			yield meta;
		yield None;

	@staticmethod
	def parseItem(core,data,each):
		soup = BeautifulSoup(each['gsjc'],"lxml");
		item = {}
		#编号
		item['code'] = each['zqdm']
		#详情
		item['details'] = soup.a.get('href')
		#简称
		item['name'] = soup.a.get_text();
		#全称
		item['fullName'] = each['gsqc']
		#行业
		item['industry'] =  each['sshymc']
		#官网
		item['url'] = each['http']
		#扩展
		item["meta"] = data['metadata']['name']

		item["type"] = core['type'];
		return item
#指定日期日线
class HistoryDay:
	@staticmethod
	def sub(core,meta,data,item):
		key = meta['key']
		if 'code' in meta:
			code = meta['code']
		else:
			code = item['code']
		date = Cache.get(code + "_historyDay",'1990-12-01');
		# yield core['get'](core,{
		# 	'key':key,
		# 	'code':code,
		# 	'date':date
		# });
		yield Core.getMeta(core,{
			'key':key,
			'code':code,
			'date':date
		})
	@staticmethod
	def parse(core,meta,body):
		js = json.loads(body)
		data = js[0]['data']

		if len(data) == 1:
			each = js[0]['data'][0]
			# "jyrq":"交易日期","zqdm":"证券代码",
			# "zqjc":"证券简称","qss":"前收",
			# "ss":"今收","sdf":"升跌<br>(%)",
			# "cjje":"成交金额<br>(万元)","syl1":"市盈率"
			item = {}
			item["code"] = each["zqdm"];
			item["name"] = each["zqjc"];
			item["date"] = each["jyrq"];
			item["settlement"] = each["qss"];
			item["trade"] = each["ss"];
			item["changepercent"] = each["sdf"];
			item["amount"] = each["cjje"];
			item["pb"] = each["syl1"];
			yield item;
		yield core['end'](core,meta,data);
	@staticmethod
	def end(core,meta,data):
		if 'scrapy:loop' in meta:
			if not meta['scrapy:loop']:
				yield None;
				return;

		code = meta["code"];
		date = meta["date"];

		now = datetime.datetime.now()
		now = now.strftime('%Y-%m-%d');

		d = datetime.datetime.strptime(date, '%Y-%m-%d')
		delta = datetime.timedelta(days=1)

		d = d + delta;
		date = d.strftime('%Y-%m-%d')

		while date < now:
			Cache.set(code + '_historyDay',date);

			meta = copy.deepcopy(meta);
			meta['scrapy:loop'] = False;
			meta['date'] = date;
			# yield core['get'](core,meta);
			yield meta;

			d = d + delta;
			date = d.strftime('%Y-%m-%d')
		yield None;
#股票行情
class Quotation:
	@staticmethod
	def sub(core,meta,data,item):
		soup = BeautifulSoup(data['jqhq'],'lxml')
		url = soup.a.get('a-param');
		# yield core['get'](core,{'url':url})
		yield Core.getMeta(core,{
			'url':url
		})
	@staticmethod
	def parse(core,meta,body):
		js = json.loads(body) 
		for data in js:
			for each in data['data']:
				yield Quotation.parseItem(core,meta,each);
	@staticmethod
	def parseItem(core,meta,each):
		item = {};
		item['date'] = each['jyrq'];
		item['code'] = each['zqdm'];
		item['name'] = each['zqjc'];
		item['settlement'] = each['qss'];
		item['trade'] = each['ss'];
		item['changepercent'] = each['sdf'];
		item['amount'] = each['cjje'];
		item['pb'] = each['syl1'];
		item['type'] = core['type']
		return item;
#公司信息
class Company:
	@staticmethod
	def sub(core,meta,data,item):
		key = meta['key']
		code = item['code'];
		# yield core['get'](core,{'key':key,'code':code})
		yield Core.getMeta(core,{
			'key':key,
			'code':code
		})
	@staticmethod
	def parse(core,meta,body):
		js = json.loads(body)
		if js['code'] == '0' and js['data'] != None:
			data = js['data']
			cols = js['cols']
			item = {}
			for key in js['cols'].keys():
				# logger.info(js['data'][key])
				item[cols[key]] = data[key]

			item['full'] = js['data']['gsqc'];
			item['type'] = core['type'];
			yield item
			
			#A股票
			code = data['agdm'];
			date = data['agssrq'];
			key = code + "_historyDay"
			if not code  == '' and  not Cache.exists(key):
				Cache.set(key,date);

			#B股票
			code = data['bgdm']
			date = data['bgssrq']
			key = code + "_historyDay"
			if not code  == '' and  not Cache.exists(key):
				Cache.set(key,date);

			yield Core.subDone(core,meta,data,item);
#关键指标
class Index:
	@staticmethod
	def sub(core,meta,data,item):
		key = meta['key']
		code = item['code'];
		# yield core['get'](core,{'key':key,'code':code})
		yield Core.getMeta(core,{
			'key':key,
			'code':code
		})
	@staticmethod
	def parse(core,meta,body):
		js = json.loads(body)

		if js['code'] == '0':
			now = js['data'][0]
			last = js['data'][1]
			change = js['data'][2]
			#总成交金额 （亿元） cjje
			#总成交量 （亿股） cjbs
			#股票总股本（亿股） zgb
			#股票流通股本（亿股） ltgb
			#股票总市值（亿元） sjzz
			#股票流通市值（亿元） ltsz
			#平均市盈率  syl
			#平均换手率 hsl
			item = {}
			item['now'] = {
				'amount':now['now_'+ 'cjje'],
				'volume':now['now_'+ 'cjbs'],
				'capital':now['now_'+ 'zgb'],
				'flowcapital':now['now_'+ 'ltgb'],
				'mktcap':now['now_'+ 'sjzz'],
				'nmc':now['now_'+ 'ltsz'],
				'pb':now['now_'+ 'syl'],
				'turnoverratio':now['now_'+ 'hsl']
			}
			item['last'] = {
				'amount':last['last_'+ 'cjje'],
				'volume':last['last_'+ 'cjbs'],
				'capital':last['last_'+ 'zgb'],
				'flowcapital':last['last_'+ 'ltgb'],
				'mktcap':last['last_'+ 'sjzz'],
				'nmc':last['last_'+ 'ltsz'],
				'pb':last['last_'+ 'syl'],
				'turnoverratio':last['last_'+ 'hsl']
			}
			item['change'] = {
				'amount':change['change_'+ 'cjje'],
				'volume':change['change_'+ 'cjbs'],
				'capital':change['change_'+ 'zgb'],
				'flowcapital':change['change_'+ 'ltgb'],
				'mktcap':change['change_'+ 'sjzz'],
				'nmc':change['change_'+ 'ltsz'],
				'pb':change['change_'+ 'syl'],
				'turnoverratio':change['change_'+ 'hsl']
			}
			item['lastDate'] = js['lastDate']
			item['code'] = meta['code']

			item['type'] = core['type'];
			yield item;
#公告
class AnnIndex:
	@staticmethod
	def sub(core,meta,data,item):
		key = meta['key']
		code = item['code'];
		#最新公告
		# yield core['get'](core,{'key':key,'code':code,"channel":"listedNotice_disc"})
		yield Core.getMeta(core,{
			'key':key,
			'code':code,
			"channel":"listedNotice_disc"
		})
		#定期报告
		# yield core['get'](core,{'key':key,'code':code,"channel":"fixed_disc"})
		yield Core.getMeta(core,{
			'key':key,
			'code':code,
			"channel":"fixed_disc"
		})
	@staticmethod
	def parse(core,meta,body):
		js = json.loads(body)
		code = meta['code']
		for each in js['data']:
			item = {}
			item['code'] = code;
			item['type'] = core['type'];

			item['title'] = each['title']
			item['publishTime'] = each['publishTime']
			item['attachPath'] = each['attachPath']
			item['attachFormat'] = each['attachFormat']

			yield item;
#市场行情数据
class Market:
	@staticmethod
	def sub(core,meta,data,item):
		key = meta['key']
		code = item['code'];
		# return core['get'](core,{'key':key,'code':code})
		yield Core.getMeta(core,{
			'key':key,
			'code':code,
		})
	@staticmethod
	def parse(core,meta,body):
		js = json.loads(body)
		if js['code'] != '0':
			yield None
			return;
		data = js['data'];
		data['type'] = core['type']
		yield data
#历史数据
class History:
	@staticmethod
	def sub(core,meta,data,item):
		key = meta['key']
		code = item['code'];
		#日线
		# yield core['make'](core,{'key':key,'code':code,'type':32})
		#周线
		# yield core['make'](core,{'key':key,'code':code,'type':33})
		#月线
		# yield core['make'](core,{'key':key,'code':code,'type':34})

		yield Core.getMeta(core,{
			'key':key,
			'code':code,
			'type':32
		})
		yield Core.getMeta(core,{
			'key':key,
			'code':code,
			'type':33
		})
		yield Core.getMeta(core,{
			'key':key,
			'code':code,
			'type':34
		})
	@staticmethod
	def parse(core,meta,body):
		cycle = meta['type'];
		if cycle == 32:
			cycle = 'day'
		elif cycle == 33:
			cycle = 'week'
		elif cycle == 34:
			cycle = 'month'

		js = json.loads(body)
		if js['code'] == '0':
			data = js['data']
			#成交量
			for each in data['picdowndata']:
				item = {}
				item["type"] = 'Volume';
				item["code"] = data['code'];
				item["cycle"] = cycle;

				#时间
				item['date'] = each[0]
				#成交量
				item['volume'] = each[1]
				#涨跌状态(minus:跌 plus:升)
				item['status'] = each[2]
				
				yield item;
			#交易数据
			for each in data['picupdata']:
				item = {}
				item["type"] = 'Transaction';
				item["code"] = data['code'];
				item["cycle"] = cycle;
				#时间
				item['date'] = each[0]
				#开盘
				item['open'] = each[1]
				#最高
				item['high'] = each[2]
				#最低
				item['low'] = each[3]
				#收盘
				item['trade'] = each[4]
				#涨跌
				item['pricechange'] = each[5]
				#涨幅
				item['changepercent'] = each[6]
				#成交量
				item['volume'] = each[7]
				#成交额
				item['amount'] = each[8]
				yield item;
#历史公告
class AnnList:
	@staticmethod
	def sub(core,meta,data,item):
		key = meta['key']
		code = item['code'];
		# return core['get'](core,{'pageNum':1,'pageSize':30,'code':code})
		yield Core.getMeta(core,{
			'pageNum':1,
			'pageSize':30,
			'code':code
		})
	@staticmethod
	def get(core,param):
		formdata = {
			'channelCode': ["listedNotice_disc"],
			'pageNum': param['pageNum'],
			'pageSize': param['pageSize'],
			'seDate': ["", ""],
			'stock': [param['code']]
		}
		yield {
			'url' : core['url'],
			'method':"POST",
			'headers':{'Content-Type': 'application/json'},
			'body':json.dumps(formdata),
			'meta' : Core.getMeta(core,param)
		}
	@staticmethod
	def parse(core,meta,body):
		pass
		js = json.loads(body);
		
		pageNum = int(meta['pageNum'])
		pagesize = int(meta['pageSize'])
		totalCount = int(js['announceCount']);

		index = totalCount - (pageNum-1)*pagesize;

		for each in js["data"]:
			index-=1
			item = {}
			item['type'] = core['type'];
			item['code'] = each["secCode"][0];
			item['sortID'] = index;
			item['title'] = each['title']
			item['publishTime'] = each['publishTime']
			item['attachPath'] = each['attachPath']
			item['attachFormat'] = each['attachFormat']
			item['attachSize'] = each['attachSize']
			yield item;
		yield core['end'](core,meta,js);
	@staticmethod
	def end(core,meta,data):
		if 'scrapy:loop' in meta:
			if not meta['scrapy:loop']:
				yield None;
				return;

		pageNum = int(meta['pageNum'])
		pagesize = int(meta['pageSize'])
		totalCount = int(data['announceCount']);
		index = totalCount - (pageNum-1)*pagesize;
		if "totalCount" in meta:
			totalCount = int(meta['totalCount']);

		while pageNum * pagesize < totalCount:
			pageNum = pageNum + 1;
			meta = copy.deepcopy(meta);
			meta['scrapy:loop'] = False
			meta['pageNum'] = pageNum;
			meta['totalCount'] = totalCount;
			# yield core['get'](core,meta);
			yield meta
		yield None;

cores = [
{
	'scrapy:request':{
		'priority':9,
	},
	'type':'Report',
	'url':Template("http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1110x&TABKEY=${key}&PAGENO=${pageno}"),
	'get':Core.get,
	'parse':Report.parse,
	'end':Report.end,
	'subItem':[
		# 'Quotation',
		# 'Company',
		# 'Index',
		# 'AnnIndex',
		# 'Market',
		# 'History',
		# 'AnnList'
	]
},
{
	'scrapy.request':{
		'priority':7,
	},
	'type':'HistoryDay',
	'url':Template('http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1815_stock&TABKEY=${key}&radioClass=00%2C20%2C30&txtSite=all&txtDMorJC=${code}&txtBeginDate=${date}&txtEndDate=${date}'),
	'get':Core.get,
	'parse':HistoryDay.parse,
	'end':HistoryDay.end,
	'subGet':HistoryDay.sub,
},
{
	'scrapy.request':{
		'priority':6,
	},
	'type':'Quotation',
	'url':Template('http://www.szse.cn/api/report${url}'),
	'get':Core.get,
	'parse':Quotation.parse,
	'subGet':Quotation.sub,
},
{
	'scrapy.request':{
		'priority':5,
	},
	'type':'Company',
	'url':Template('http://www.szse.cn/api/report/index/companyGeneralization?secCode=${code}'),
	'get':Core.get,
	'parse':Company.parse,
	'subGet':Company.sub,
	'subItem':[
		'HistoryDay'
	]
},
{
	'scrapy.request':{
		'priority':4,
	},
	'type':'Index',
	'url':Template('http://www.szse.cn/api/report/index/stockKeyIndexGeneralization?secCode=${code}'),
	'get':Core.get,
	'parse':Index.parse,
	'subGet':Index.sub
},
{
	'scrapy.request':{
		'priority':3,
	},
	'type':'AnnIndex',
	'url':Template('http://www.szse.cn/api/disc/announcement/annIndex?secCode=${code}&channelCode=${channel}'),
	'get':Core.get,
	'parse':AnnIndex.parse,
	'subGet':AnnIndex.sub
},
{
	'scrapy.request':{
		'priority':2,
	},
	'type':'Market',
	'url':Template('http://www.szse.cn/api/market/ssjjhq/getTimeData?code=${code}&marketId=1'),
	'get':Core.get,
	'parse':Market.parse,
	'subGet':Market.sub
},
{
	'scrapy.request':{
		'priority':1,
	},
	'type':'History',
	'url':Template('http://www.szse.cn/api/market/ssjjhq/getHistoryData?code=${code}&marketId=1&cycleType=${type}'),
	'get':Core.get,
	'parse':History.parse,
	'subGet':History.sub
},
{
	'scrapy:request':{
		'priority':0,
	},
	'type':'AnnList',
	'url':'http://www.szse.cn/api/disc/announcement/annList',
	'get':AnnList.get,
	'parse':AnnList.parse,
	'end':AnnList.end,
	'subGet':AnnList.sub
}
]

def internal_findCore(type_):
	for core in cores:
		if type_ == core['type']:
			return core;
	return None;

def findCore(meta):
	if not meta['scrapy:domain'] == static_domain:
		return None;
	return internal_findCore(meta['scrapy:type'])