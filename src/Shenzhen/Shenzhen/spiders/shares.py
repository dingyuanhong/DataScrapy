# -*- coding: utf-8 -*-
import scrapy
import random
from string import Template
from Shenzhen.items import CodeItem,QuotationItem
import json
from bs4 import BeautifulSoup
import datetime
import redis
import os
import sys
sys.path.append("..")
from util.logger import getLogger
from scrapy_redis.spiders import RedisSpider

class SharesSpider(RedisSpider):
    name = 'shares'
    allowed_domains = ['szse.cn']
    
    logger = getLogger('Shenzhen');

    #股票代码
    url = Template("http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1110x&TABKEY=${key}&PAGENO=${pageno}&random=${random}")
    keys = ['tab1','tab2','tab3','tab4'];

    #历史日线
    historyDay = Template('http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1815_stock&TABKEY=${key}&radioClass=00%2C20%2C30&txtSite=all&txtDMorJC=${code}&txtBeginDate=${date}&txtEndDate=${date}&random=${random}')

    #公司信息
    company = Template('http://www.szse.cn/api/report/index/companyGeneralization?secCode=${code}&random=${random}')
    #关键指标
    IndexGeneralization = Template('http://www.szse.cn/api/report/index/stockKeyIndexGeneralization?secCode=${code}&random=${random}')
    #最新公告
    annIndex = Template('http://www.szse.cn/api/disc/announcement/annIndex?secCode=${code}&random=${random}&channelCode=${channel}')
    #市场行情数据
    market = Template('http://www.szse.cn/api/market/ssjjhq/getTimeData?code=${code}&random=${random}&marketId=1')
    #历史数据
    history = Template('http://www.szse.cn/api/market/ssjjhq/getHistoryData?code=${code}&random=${random}&marketId=1&cycleType=${type}')
    #公告
    annList = 'http://www.szse.cn/api/disc/announcement/annList'
    
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
    cache = redis.Redis(connection_pool=pool)

    def start_requests(self):
        pageno = 1;
        for key in self.keys:
            meta = {'key':key,'pageno':pageno,'random':random.random()}
            url = self.url.substitute(meta)
            yield scrapy.Request(url,meta=meta,callback= self.parse)
            # return;

    def parse(self, response):
        self.logger.info(response.url);
        key = response.meta['key']
        js = json.loads(response.body) 
        for data in js:
            if data['metadata']['tabkey'] == key:
                for each in data['data']:
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

                    item["type"] = "generalization";

                    yield item

                    code = item['code'];

                    # 历史数据 指定日期日线
                    date = self.getDay(code,"historyDay");
                    meta = {
                        'key':key,
                        'code':code,
                        'date':date,
                        'random':random.random()
                    };
                    yield scrapy.Request(self.historyDay.substitute(meta),
                        meta=meta,
                        callback = self.parseHistoryDay)

                    if key != 'tab1':
                        continue;

                    #股票行情
                    soup = BeautifulSoup(each['jqhq'],'lxml')
                    url = soup.a.get('a-param');
                    url = 'http://www.szse.cn/api/report' + url;
                    yield scrapy.Request(url,
                        callback = self.parseQuotation)

                    # #公司信息
                    meta = {'key':key,'code':code,'random':random.random()};
                    yield scrapy.Request(self.company.substitute(meta),
                        meta=meta,
                        callback = self.parseCompany)

                    # #关键指标
                    yield scrapy.Request(self.IndexGeneralization.substitute(meta),
                        meta=meta,
                        callback = self.parseIndex)

                    meta = {'key':key,'code':code,'random':random.random(),"channel":"listedNotice_disc"};
                    # #最新公告
                    yield scrapy.Request(self.annIndex.substitute(meta),
                        meta=meta,
                        callback = self.parseAnnIndex)

                    meta = {'key':key,'code':code,'random':random.random(),"channel":"fixed_disc"};
                    # #定期报告
                    yield scrapy.Request(self.annIndex.substitute(meta),
                        meta=meta,
                        callback = self.parseAnnIndex)

                    meta = {'key':key,'code':code,'random':random.random()};
                    # #市场行情数据
                    yield scrapy.Request(self.market.substitute(meta),
                        meta=meta,
                        callback = self.parseMarket)

                    meta = {'key':key,'code':code,'random':random.random(),'type':32};
                    #历史数据 日线
                    yield scrapy.Request(self.history.substitute(meta),
                        meta=meta,
                        callback = self.parseHistory)

                    meta = {'key':key,'code':code,'random':random.random(),'type':33};
                    # #历史数据 周线
                    yield scrapy.Request(self.history.substitute(meta),
                        meta=meta,
                        callback = self.parseHistory)

                    meta = {'key':key,'code':code,'random':random.random(),'type':34};
                    # #历史数据 月线
                    yield scrapy.Request(self.history.substitute(meta),
                        meta=meta,
                        callback = self.parseHistory)

                    #公告
                    # "fixed_disc"
                    formdata = {
                        'channelCode': ["listedNotice_disc"],
                        'pageNum': '1',
                        'pageSize': '30',
                        'seDate': ["", ""],
                        'stock': [code]
                    }
                    yield scrapy.FormRequest(
                        url = self.annList +'?random='+str(random.random()),
                        method="POST",
                        headers={'Content-Type': 'application/json'},
                        body=json.dumps(formdata),
                        meta = formdata,
                        callback = self.parseAnnList
                    );
                
                if data['metadata']['pageno'] * data['metadata']['pagesize'] < data['metadata']['recordcount']:
                    # 每次处理完一页的数据之后，重新发送下一页页面请求
                    # self.offset自增10，同时拼接为新的url，并调用回调函数self.parse处理Response
                    pageno = response.meta['pageno'] + 1;
                    meta = {'key':key,'pageno':pageno,'random':random.random()}
                    yield scrapy.Request(self.url.substitute(meta),
                        meta=meta,
                        callback = self.parse)

    #股票指数
    def parseQuotation(self, response):
        js = json.loads(response.body) 
        for data in js:
            for each in data['data']:
                item = {};
                item['date'] = each['jyrq'];
                item['code'] = each['zqdm'];
                item['name'] = each['zqjc'];
                item['settlement'] = each['qss'];
                item['trade'] = each['ss'];
                item['changepercent'] = each['sdf'];
                item['amount'] = each['cjje'];
                item['pb'] = each['syl1'];
                item['type'] = 'quotation'

                yield item;

    #公司信息
    def parseCompany(self,response):
        js = json.loads(response.body)
        if js['code'] == '0' and js['data'] != None:
            item = {}
            for key in js['cols'].keys():
                # self.logger.info(js['data'][key])
                item[js['cols'][key]] = js['data'][key]

            item['full'] = js['data']['gsqc'];
            item['type'] = 'company';
            yield item

    #关键指数
    def parseIndex(self,response):
        js = json.loads(response.body)
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
            item['code'] = response.meta['code']

            item['type'] = 'index';
            yield item;

    #最新公告
    def parseAnnIndex(self,response):
        js = json.loads(response.body)
        code = response.meta['code']
        for each in js['data']:
            item = {}
            item['code'] = code;
            item['type'] = 'annIndex';
            item['title'] = each['title']
            item['publishTime'] = each['publishTime']
            item['attachPath'] = each['attachPath']
            item['attachFormat'] = each['attachFormat']

            yield item;

    #市场分时数据----一分钟一次实时调用
    def parseMarket(self,response):
        self.logger.info(response.url);
        js = json.loads(response.body)
        if js['code'] != '0':
            return;
        data = js['data'];
        data['type'] = 'market'
        yield data

        #市场时间
        # data['datatime'];
        #时间
        data['marketTime'];
        #代码
        data['code'];
        #名称
        data['name'];
        #昨收
        data['close'];
        #涨跌
        data['delta'];
        #涨幅
        data['deltaPercent']
        #最高价
        data['high']
        #最低价
        data['low']
        #开盘价
        data['open']
        #现价
        data['now']
        #成交额
        data['amount']
        #成交量 (手)
        data['volume']
        #昨日成交量 (手)
        data['lastVolume']

        #分钟级数据
        #均价 分钟
        data['picavgprice']
        #成交量 分钟
        data['picdowndata']
        #详细数据 分钟
        data['picupdata']
        # [
        # 0:时间
        # 1：最新价
        # 2：均价
        # 3：涨跌
        # 4：涨幅
        # 5：成交量
        # 6：成交额
        # ]

        #买盘 卖盘 前5是卖 后5是买
        data['sellbuy5']

    #市场历史数据
    def parseHistory(self,response):
        cycle = response.meta['type'];
        if cycle == 32:
            cycle = 'day'
        elif cycle == 33:
            cycle = 'week'
        elif cycle == 34:
            cycle = 'month'

        js = json.loads(response.body)
        if js['code'] == '0':
            data = js['data']
            #成交量
            for each in data['picdowndata']:
                item = {}
                item["type"] = 'volume';
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
                item["type"] = 'transaction';
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

    def getDay(self,code,type_):
        if type_ == 'historyDay':
            date = self.cache.get(type_ +"-"+ code);
        if date == None:
            return "1990-12-01";
        return str(date.decode('utf-8'));

    def setDay(self,code,type_,day):
        if type_ == 'historyDay':
            return self.cache.set(type_ +"-"+ code,day);

    #获取指定日期数据
    def parseHistoryDay(self,response):
        js = json.loads(response.body)
        data = js[0]['data']

        if len(data) == 1:
            each = js[0]['data'][0]
            # "jyrq":"交易日期","zqdm":"证券代码",
            # "zqjc":"证券简称","qss":"前收",
            # "ss":"今收","sdf":"升跌<br>(%)",
            # "cjje":"成交金额<br>(万元)","syl1":"市盈率"
            item = {}
            item["type"] = "historyDay";
            item["code"] = each["zqdm"];
            item["name"] = each["zqjc"];
            item["date"] = each["jyrq"];
            item["settlement"] = each["qss"];
            item["trade"] = each["ss"];
            item["changepercent"] = each["sdf"];
            item["amount"] = each["cjje"];
            item["pb"] = each["syl1"];
            yield item;

        key = response.meta["key"];
        code = response.meta["code"];

        date = response.meta["date"];
        d = datetime.datetime.strptime(date, '%Y-%m-%d')
        delta = datetime.timedelta(days=1)
        d = d + delta;
        date = d.strftime('%Y-%m-%d')

        self.setDay(code,"historyDay",date);

        now = datetime.datetime.now()
        now = now.strftime('%Y-%m-%d');
        if date < now:
            meta = {'key':key,'code':code,'date':date,'random':random.random()}
            yield scrapy.Request(self.historyDay.substitute(meta),
                meta=meta,
                callback = self.parseHistoryDay)

    #公告
    def parseAnnList(self,response):
        pageNum = int(response.meta['pageNum'])
        pagesize = int(response.meta['pageSize'])
        js = json.loads(response.body);

        totalCount = js['announceCount'];
        index = totalCount - (pageNum-1)*pagesize;
        for each in js["data"]:
            index-=1
            item = {}
            item['code'] = each["secCode"][0];
            item['type'] = 'annIndex';
            item['sortID'] = index;
            item['title'] = each['title']
            item['publishTime'] = each['publishTime']
            item['attachPath'] = each['attachPath']
            item['attachFormat'] = each['attachFormat']
            item['attachSize'] = each['attachSize']
            yield item;

        if "totalCount" in response.meta:
            totalCount = response.meta['totalCount'];

        if pageNum * pagesize < int(totalCount):
            formdata = response.meta;
            formdata['pageNum'] = pageNum + 1;
            formdata['totalCount'] = totalCount;
            yield scrapy.FormRequest(
                url = self.annList +'?random='+str(random.random()),
                method="POST",
                headers={'Content-Type': 'application/json'},
                body=json.dumps(formdata),
                meta = formdata,
                callback = self.parseAnnList
            );
