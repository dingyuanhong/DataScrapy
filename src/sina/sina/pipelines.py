# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import logging
from pymongo import MongoClient
import redis

logger = logging.getLogger('Sina')

class SinaPipeline(object):
    def __init__(self):
        self.conn = MongoClient('127.0.0.1', 27017)
        self.db = self.conn.Sina
        self.DayClosingData = self.db.DayClosingData  #日收盘数据
        self.real = self.db.real  #实时数据
        self.minute_5 = self.db.minute_5  #5分钟数据
        self.minute_15 = self.db.minute_15  #15分钟数据
        self.minute_30 = self.db.minute_30  #30分钟数据
        self.minute_60 = self.db.minute_60  #60分钟数据
        self.quarter = self.db.quarter;     #历史交易明细
        self.capitalflow = self.db.capitalflow  #资金出入
        self.financesummary = self.db.financesummary  #财务摘要

        self.TencentMinute = self.db.TencentMinute  #腾讯分钟数据
        self.TencentDay = self.db.TencentDay  #腾讯日数据
        self.TencentDayK = self.db.TencentDayK  #腾讯日K数据
        self.TencentYearDayK = self.db.TencentYearDayK  #腾讯年日K数据
        self.TencentWeekK = self.db.TencentWeekK  #腾讯周K数据
        self.TencentMonthK = self.db.TencentMonthK  #腾讯周K数据
        self.TencentClosingDetails = self.db.TencentClosingDetails   #成交明细

    def process_item(self, item, spider):
        if "type" not in item:
            return item;
        if item["type"] == "DayClosingData":
            self.DayClosingData.update({'symbol':item['symbol'],'date':item['date'] ,'ticktime':item['ticktime'] },{'$set':item},True)
        if item["type"] == "real":
            self.real.update({'symbol':item['symbol'],'date':item['date'] ,'time':item['time'] },{'$set':item},True)
        if item["type"] == "minute_5":
            self.minute_5.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "minute_15":
            self.minute_15.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "minute_30":
            self.minute_30.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "minute_60":
            self.minute_60.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "capitalflow":
            self.capitalflow.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "financesummary":
            self.financesummary.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "TencentMinute":
            self.TencentMinute.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "TencentDay":
            self.TencentDay.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "TencentDayK":
            self.TencentDayK.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "TencentYearDayK":
            self.TencentYearDayK.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "TencentWeekK":
            self.TencentWeekK.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "TencentMonthK":
            self.TencentMonthK.update({'symbol':item['symbol'],'date':item['date']},{'$set':item},True)
        if item["type"] == "TencentClosingDetails":
            self.TencentClosingDetails.update({'symbol':item['symbol'],'time':item['time']},{'$set':item},True)
        
        return item
