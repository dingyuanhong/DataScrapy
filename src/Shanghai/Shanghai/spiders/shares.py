# -*- coding: utf-8 -*-
import scrapy
import random
from string import Template
import logging
import json
from bs4 import BeautifulSoup
import urllib.parse
import time

def log(name):
    logger = logging.getLogger(name)
    # file = logging.FileHandler("./log/Shenzhen.log");
    # file.setLevel(logging.INFO);
    # logger.addHandler(file);
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(relativeCreated)d [%(name)s] %(levelname)s:%(message)s')
    console.setFormatter(formatter)
    
    logger.addHandler(console);
    return logger;

#组合参数
def MergeParam(keys,values):
    param = ''
    for key in keys:
    	if param != '':
    		param += '&'
    	v = str(values[key])
    	param += str(key) +'='+ urllib.parse.quote(v)
    return param;

class SharesSpider(scrapy.Spider):
    name = 'shares'
    allowed_domains = ['sse.com.cn']
    logger = log('Shanghai');

    url = "http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/equity";
    keys = ["callback","select","order","begin","end","_"]

    def start_requests(self):
        
        values = {
        	"callback":"",
        	"select":"code,name,open,high,low,last,prev_close,chg_rate,volume,amount,tradephase,change,amp_rate",
        	"order":"",
        	"begin":0,
        	"end":25,
        	"pageno":25,
        	"_":int(time.time()*1000)
        };

        url = self.url + "?" + MergeParam(self.keys,values);
        # self.logger.info(url);
        yield scrapy.Request(url,meta=values,callback= self.parse)

    def parse(self, response):
        pass
        data = response.body[1:][:-1].decode('gbk');
        # self.logger.info(data);
        js = json.loads(data) 
        self.logger.info(len(js["list"]))
        for each in js['list']:
        	#代码
        	#简称
        	#开盘
        	#最高
        	#最低
        	#最新
        	#前收
        	#涨跌幅
        	#成交量
        	#成交额
        	#-------公式:T111
        	#涨跌
        	#振幅
        	each[0];
        	each[1];

        #年月日
        js["date"]; #YYYYMMDD
        #时分秒
        js["time"]; #HHMMSS

        begin = int(js["begin"])
        end = int(js["end"])
        total = int(js["total"])
        # self.logger.info([begin,end , total]);
        if end < total:
        	values = response.meta
        	values["begin"] = end;
        	values["end"] = end + int(values['pageno']);
        	values["_"] = int(time.time()*1000);
        	url = self.url + "?" + MergeParam(self.keys,values);
        	yield scrapy.Request(url,meta=values,callback= self.parse)

