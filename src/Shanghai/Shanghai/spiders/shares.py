# -*- coding: utf-8 -*-
import scrapy
import random
from string import Template
import json
from bs4 import BeautifulSoup
import urllib.parse
import time
import sys
sys.path.append("..")
from util.logger import getLogger
from util.params import Merge as MergeParam

class SharesSpider(scrapy.Spider):
    name = 'shares'
    allowed_domains = ['sse.com.cn']
    logger = getLogger('Shanghai');

    url = "http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/equity";
    snap = "http://yunhq.sse.com.cn:32041/v1/sh1/snap/";
    line = 'http://yunhq.sse.com.cn:32041/v1/sh1/line/'
    kline = 'http://yunhq.sse.com.cn:32041/v1/sh1/dayk/'

    def start_requests(self):
        begin = 0;
        param = self.getCode_requests(begin)
        yield scrapy.Request(param['url'],meta=param['meta'],callback= self.parseCodes)

    #股票代码
    def getCode_requests(self,begin):
        keys = ["callback","select","order","begin","end","_"]
        values = {
            "callback":"",
            "select":"code,name,open,high,low,last,prev_close,chg_rate,volume,amount,tradephase,change,amp_rate",
            "order":"",
            "begin":begin,
            "end":begin+25,
            "pageno":25,
            "_":int(time.time()*1000)
        };

        url = self.url + "?" + MergeParam(keys,values);
        # self.logger.info(url);
        return {
	        'url':url,
	        'meta':values
        }

    def parseCodes(self, response):
        data = response.body[1:][:-1].decode('gbk');
        # self.logger.info(data);
        js = json.loads(data) 
        for each in js['list']:
            item = {}
            item['type'] = 'code';
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

            param = self.getSnap_requests(each[0])
            yield scrapy.Request(param['url'],meta=param['meta'],callback= self.parseSnap)
            param = self.getLine_requests(each[0]);
            yield scrapy.Request(param['url'],meta=param['meta'],callback= self.parseLine)
            param = self.getKLine_requests(each[0]);
            yield scrapy.Request(param['url'],meta=param['meta'],callback= self.parseKLine)

        return;

        begin = int(js["begin"])
        end = int(js["end"])
        total = int(js["total"])
        self.logger.info([begin,end , total]);
        if end < total:
            param = self.getCode_requests(end);
            yield scrapy.Request(param['url'],meta=param['meta'],callback= self.parseCodes)

    #实时交易数据
    def getSnap_requests(self,code):
        keys = ["callback","select","_"]
        values = {
            "callback":"",
            "select":"name,last,chg_rate,change,amount,volume,open,prev_close,ask,bid,high,low,tradephase",
            "_":int(time.time()*1000)
        };
        url = self.snap + str(code) + "?" + MergeParam(keys,values);
        # self.logger.info(url);
        return {
            'url':url,
            'meta':values
        }
        # yield scrapy.Request(url,meta=values,callback= self.parseSnap)

    def parseSnap(self,response):
        data = response.body[1:][:-1].decode('gbk');
        js = json.loads(data)

        item = {}
        item['type'] = 'snap';
        item['date'] = js["date"];
        item['time'] = js["time"];
        item['code'] = js["code"];
        
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
        each = js['snap']
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

    #分时线
    def getLine_requests(self,code):
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
            "_":int(time.time()*1000)
        };
        url = self.line + str(code) + "?" + MergeParam(keys,values);
        # self.logger.info(url);
        return {
            'url':url,
            'meta':values
        }
        # yield scrapy.Request(url,meta=values,callback= self.parseLine)

    def parseLine(self,response):
        data = response.body[1:][:-1].decode('gbk');
        js = json.loads(data)

        #code: "600000"     代码
        #highest: 11.59     最高
        #lowest: 11.44      最低
        #prev_close: 11.55    昨收
        #begin: 0
        #end: 241
        #total: 241   总数
        #date: 20190321
        #time: 154508
        item = {}
        item['type'] = 'line';
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

    #日线
    def getKLine_requests(self,code):
        keys = ["callback","select","begin","end","_"]
        values = {
            "callback":"",
            "begin":0,
            "end":-1,
            "select":"date,open,high,low,close,volume",
            "_":int(time.time()*1000)
        };
        url = self.kline + str(code) + "?" + MergeParam(keys,values);
        # self.logger.info(url);
        return {
            'url':url,
            'meta':values
        }
        # yield scrapy.Request(url,meta=values,callback= self.parseKLine)

    def parseKLine(self,response):
        data = response.body[1:][:-1].decode('gbk');
        js = json.loads(data)

        # code: "600000" #代码
        # begin: 4273 #开始索引
        # end: 4572    #结束索引
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
            yield item;
