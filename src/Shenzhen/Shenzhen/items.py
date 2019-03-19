# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

#证券代码
class CodeItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    #公司代码 zqdm
    code = scrapy.Field();
    #简称 gsjc
    abbreviation = scrapy.Field();
    #全称 gsqc
    fullName = scrapy.Field();
    #行业 sshymc
    industry = scrapy.Field();
    #网址 http
    url = scrapy.Field();
    #详情 jqhq
    details = scrapy.Field();
    #类型
    meta = scrapy.Field();

#行情
class QuotationItem(scrapy.Item):
    #交易日期 jyrq
    date = scrapy.Field();
    #证券代码 zqdm
    code = scrapy.Field();
    #简称 zqjc
    abbreviation = scrapy.Field();
    #昨收 qss
    frontReceipt = scrapy.Field();
    #今收 ss
    receiveNow = scrapy.Field();
    #升跌 sdf
    updown = scrapy.Field();
    #成交金额 cjje
    transactionAmount = scrapy.Field();
    #市盈率 syl1
    ratio = scrapy.Field();

