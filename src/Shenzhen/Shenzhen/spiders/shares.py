# -*- coding: utf-8 -*-
import scrapy
import random
from string import Template
from Shenzhen.items import CodeItem,QuotationItem
import logging
import json
from bs4 import BeautifulSoup

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


class SharesSpider(scrapy.Spider):
    name = 'shares'
    allowed_domains = ['szse.cn']
    
    logger = log('Shenzhen');

    #股票代码
    url = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1110x&TABKEY=${key}&PAGENO=${pageno}&random=${random}"
    code = Template(url)
    keys = ['tab1','tab2','tab3','tab4'];

    #公司信息
    company = 'http://www.szse.cn/api/report/index/companyGeneralization'
    #关键指标
    IndexGeneralization = 'http://www.szse.cn/api/report/index/stockKeyIndexGeneralization'
    #最新公告
    annIndex = 'http://www.szse.cn/api/disc/announcement/annIndex'
    #市场行情数据
    market = 'http://www.szse.cn/api/market/ssjjhq/getTimeData'
    #历史数据
    history = 'http://www.szse.cn/api/market/ssjjhq/getHistoryData'
    #公告
    annList = 'http://www.szse.cn/api/disc/announcement/annList'
    #历史日线
    historyDay = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1815_stock&TABKEY=${key}&radioClass=00%2C20%2C30&txtSite=all&txtDMorJC=${code}&txtBeginDate=${date}&txtEndDate=${date}&random=${random}'
    day = Template(historyDay)

    def start_requests(self):
        pageno = 1;
        for key in self.keys:
            url = self.code.substitute(key=key,pageno=pageno,random=random.random())
            yield scrapy.Request(url,meta={'key':key,'pageno':pageno},callback= self.parse)
            return;

    def parse(self, response):
        key = response.meta['key']
        js = json.loads(response.body) 
        for data in js:
            if data['metadata']['tabkey'] == key:
                for each in data['data']:
                    item = CodeItem()
                    # 职位名称
                    item['code'] = each['zqdm']
                    soup = BeautifulSoup(each['gsjc'],"lxml");
                    
                    #详情
                    item['details'] = soup.a.get('href')
                    #简称
                    item['abbreviation'] = soup.a.get_text();
                    #全称
                    item['fullName'] = each['gsqc']
                    #行业
                    item['industry'] =  each['sshymc']
                    #官网
                    item['url'] = each['http']
                    #扩展
                    item["meta"] = data['metadata']['name']

                    #行情
                    soup = BeautifulSoup(each['jqhq'],'lxml')
                    url = soup.a.get('a-param');
                    url = 'http://www.szse.cn/api/report' + url;

                    # yield item

                    #股票行情
                    # yield scrapy.Request(url,
                    #     meta={'key':key},
                    #     callback = self.parseQuotation)

                    #公司信息
                    # yield scrapy.Request(self.company + '?secCode='+item['code']
                    #     +'&random='+str(random.random()),
                    #     meta={'key':key,'code':item['code']},
                    #     callback = self.parseCompany)

                    #关键指标
                    # yield scrapy.Request(self.IndexGeneralization + '?secCode='+item['code']
                    #     +'&random='+str(random.random()),
                    #     meta={'key':key,'code':item['code']},
                    #     callback = self.parseIndex)

                    #最新公告
                    # yield scrapy.Request(self.annIndex + '?secCode='+item['code']
                    #     +'&random='+str(random.random())+'&channelCode=listedNotice_disc',
                    #     meta={'key':key,'code':item['code']},
                    #     callback = self.parseAnnIndex)

                    #定期报告
                    # yield scrapy.Request(self.annIndex + '?secCode='+item['code']
                    #     +'&random='+str(random.random())+'&channelCode=fixed_disc',
                    #     meta={'key':key,'code':item['code']},
                    #     callback = self.parseAnnIndex)

                    #市场行情数据
                    # yield scrapy.Request(self.market + '?code='+item['code']
                    #     +'&random='+str(random.random())+'&marketId=1',
                    #     meta={'key':key,'code':item['code']},
                    #     callback = self.parseMarket)

                    #历史数据 日线
                    # yield scrapy.Request(self.history + '?code='+item['code']
                    #     +'&random='+str(random.random())+'&marketId=1&cycleType=32',
                    #     meta={'key':key,'code':item['code'],'type':32},
                    #     callback = self.parseHistory)

                    # #历史数据 周线
                    # yield scrapy.Request(self.history + '?code='+item['code']
                    #     +'&random='+str(random.random())+'&marketId=1&cycleType=33',
                    #     meta={'key':key,'code':item['code'],'type':33},
                    #     callback = self.parseHistory)

                    # #历史数据 月线
                    # yield scrapy.Request(self.history + '?code='+item['code']
                    #     +'&random='+str(random.random())+'&marketId=1&cycleType=34',
                    #     meta={'key':key,'code':item['code'],'type':34},
                    #     callback = self.parseHistory)

                    #历史数据 指定日期日线
                    yield scrapy.Request(self.day.substitute(
                        key=key,
                        code=item['code'],
                        date='2018-05-10',
                        random=random.random()),
                        meta={'key':key,'code':item['code']},
                        callback = self.parseHistoryDay)

                    #公告
                    # formdata = {
                    #     'channelCode': ["fixed_disc"],
                    #     'pageNum': '1',
                    #     'pageSize': '30',
                    #     'seDate': ["", ""],
                    #     'stock': [item['code']]
                    # }
                    # yield scrapy.FormRequest(
                    #     url = self.annList +'?random='+str(random.random()),
                    #     formdata = formdata,
                    #     meta = formdata,
                    #     callback = self.parseAnnList
                    # );

                    return;
                
                if data['metadata']['pageno'] * data['metadata']['pagesize'] < data['metadata']['recordcount']:
                    # 每次处理完一页的数据之后，重新发送下一页页面请求
                    # self.offset自增10，同时拼接为新的url，并调用回调函数self.parse处理Response
                    pageno = response.meta['pageno'] + 1;
                    yield scrapy.Request(self.s.substitute(key=key,pageno=pageno,random=random.random()),
                        meta={'key':key,'pageno':pageno},
                        callback = self.parse)

    #股票指数
    def parseQuotation(self, response):
        js = json.loads(response.body) 
        for data in js:
            for each in data['data']:
                item = QuotationItem();
                item['date'] = each['jyrq'];
                item['code'] = each['zqdm'];
                item['abbreviation'] = each['zqjc'];
                item['frontReceipt'] = each['qss'];
                item['receiveNow'] = each['ss'];
                item['updown'] = each['sdf'];
                item['transactionAmount'] = each['cjje'];
                item['ratio'] = each['syl1'];

                yield item;

    #公司信息
    def parseCompany(self,response):
        js = json.loads(response.body)
        if js['code'] == '0':
            item = {}
            for key in js['cols'].keys():
                # self.logger.info(js['data'][key])
                item[js['cols'][key]] = js['data'][key]
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
                'cjje':now['now_'+ 'cjje'],
                'cjbs':now['now_'+ 'cjbs'],
                'zgb':now['now_'+ 'zgb'],
                'ltgb':now['now_'+ 'ltgb'],
                'sjzz':now['now_'+ 'sjzz'],
                'ltsz':now['now_'+ 'ltsz'],
                'syl':now['now_'+ 'syl'],
                'hsl':now['now_'+ 'hsl']
            }
            item['last'] = {
                'cjje':last['last_'+ 'cjje'],
                'cjbs':last['last_'+ 'cjbs'],
                'zgb':last['last_'+ 'zgb'],
                'ltgb':last['last_'+ 'ltgb'],
                'sjzz':last['last_'+ 'sjzz'],
                'ltsz':last['last_'+ 'ltsz'],
                'syl':last['last_'+ 'syl'],
                'hsl':last['last_'+ 'hsl']
            }
            item['change'] = {
                'cjje':change['change_'+ 'cjje'],
                'cjbs':change['change_'+ 'cjbs'],
                'zgb':change['change_'+ 'zgb'],
                'ltgb':change['change_'+ 'ltgb'],
                'sjzz':change['change_'+ 'sjzz'],
                'ltsz':change['change_'+ 'ltsz'],
                'syl':change['change_'+ 'syl'],
                'hsl':change['change_'+ 'hsl']
            }
            yield item;

    #最新公告
    def parseAnnIndex(self,response):
        js = json.loads(response.body)
        yield js

    #市场实时数据
    def parseMarket(self,response):
        self.logger.info(response.url);
        js = json.loads(response.body)
        if js['code'] != '0':
            return;
        data = js['data'];
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
        js = json.loads(response.body)
        if js['code'] == '0':
            data = js['data']
            #成交量
            for each in data['picdowndata']:
                #时间
                each[0]
                #成交量
                each[1]
            #实时更新
            for each in data['picupdata']:
                #时间
                each[0]
                #开盘
                each[1]
                #最高
                each[2]
                #最低
                each[3]
                #收盘
                each[4]
                #涨跌
                each[5]
                #涨幅
                each[6]
                #成交量
                each[7]
                #成交额
                each[8]

            yield data

    #获取指定日期数据
    def parseHistoryDay(self,response):
        js = json.loads(response.body)
        yield js[0]['data'][0];

    #公告
    def parseAnnList(self,response):
        self.logger.info(response.url);

        pageNum = int(response.meta['pageNum'])
        pagesize = int(response.meta['pageSize'])
        js = json.loads(response.body);
        totalCount = js['announceCount'];

        yield js['data']

        if pageNum * pagesize < totalCount:
            formdata = response.meta;
            formdata['pageNum'] = pageNum + 1;
            yield scrapy.FormRequest(
                url = self.annList +'?random='+str(random.random()),
                formdata = formdata,
                meta = formdata,
                callback = self.parseAnnList
            );
