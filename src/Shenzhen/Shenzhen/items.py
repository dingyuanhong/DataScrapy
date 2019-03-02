# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ShenzhenItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    #公司代码
    code = scrapy.Field();
    #简称
    abbreviation = scrapy.Field();
    #全称
    fullName = scrapy.Field();
    #行业
    industry = scrapy.Field();
    #网址
    url = scrapy.Field();
    #详情
    details = scrapy.Field();


