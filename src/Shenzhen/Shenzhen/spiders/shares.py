# -*- coding: utf-8 -*-
import scrapy
import random
from string import Template
from Shenzhen.items import ShenzhenItem
import logging

class SharesSpider(scrapy.Spider):
    name = 'shares'
    allowed_domains = ['szse.cn']

    url = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1110x&TABKEY=${key}&PAGENO=${pageno}&random=${random}"
    s = Template(url)
    key = 'tab1';
    pageno = 1;
    start_urls = [s.substitute(key=key,pageno=pageno,random=random.random())];

    def parse(self, response):
        for each in response.xpath("//table[@class='table-tab1']/tr"):
            # 初始化模型对象
            item = ShenzhenItem()
            # 职位名称
            item['code'] = each.xpath("./td[0]/text()").extract()[0]
            # 详情连接
            item['details'] = each.xpath("./td[1]/a/@href").extract()[0]
            #简称
            item['abbreviation'] = each.xpath("./td[1]/a/u/text()").extract()[0]
            #全称
            item['fullName'] = each.xpath("./td[2]/text()").extract()[0]
            #行业
            item['industry'] =  each.xpath("./td[3]/text()").extract()[0]
            # 工作地点
            item['url'] = each.xpath("./td[4]/text()").extract()[0]

            logging.info(item);
            yield item

        # 每次处理完一页的数据之后，重新发送下一页页面请求
        # self.offset自增10，同时拼接为新的url，并调用回调函数self.parse处理Response
        self.pageno = self.pageno + 1;
        yield scrapy.Request(self.s.substitute(key=self.key,pageno=self.pageno,random=random.random()), callback = self.parse)