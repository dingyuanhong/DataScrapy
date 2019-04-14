# -*- coding: utf-8 -*-
import scrapy
import random
from string import Template
import logging
import json
import demjson
from bs4 import BeautifulSoup
import urllib.parse
import time
import re
import types
import sys
sys.path.append("..")
from util.logger import getLogger
from scrapy_redis.spiders import RedisSpider

def getLastClosingDate():
    return time.strftime('%Y%m%d',time.localtime(time.time()))

class SharesSpider(scrapy.Spider):
    name = 'shares'
    logger = getLogger('Sina');

    allowed_domains = ['sina.com','*.sina.com.cn','sina.com.cn','hq.sinajs.cn'
        ,'data.gtimg.cn','stock.gtimg.cn','stock.finance.qq.com']
    
    url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"

    #http://vip.stock.finance.sina.com.cn/mkt/
    codes = {
        'count':'Market_Center.getHQNodeStockCount',
        'data':'Market_Center.getHQNodeData',
        'nodes':'Market_Center.getHQNodes'
    }

    nodes = {};

    def start_requests(self):
        param = self.getNodes_request()
        yield scrapy.Request(param['url'],callback= self.parseNodes)
        
    def getNodes_request(self):
        code = self.codes['nodes']
        url = self.url + '/' + code;
        return {
            'url':url
        }

    def getCount_request(self,node,tag):
        code = self.codes['count']
        url = self.url + '/' + code + '?' +  'node='+node;
        return {
            'url':url,
            'meta': {
                'node':node,
                'tag':tag
            }
        }

    def getData_request(self,node,tag,page,count):
        code = self.codes['data']
        svcParam = {'page': page, 'num': 80, 'sort': 'symbol', 'asc': 1, 'node': node,'symbol':''};
        params ='&'.join([k + '=' + str(v) for k,v in svcParam.items()])
        url = self.url + '/' + code + '?' +  params;
        return {
            'url':url,
            'meta':{
                'node':node,
                'count':count,
                'page':page,
                'tag':tag
            }
        }

    def makeNodes(self,arr):
        result = {}
        if len(arr) >= 6:
            name = arr[0]
            soup = BeautifulSoup(name,"lxml");
            name = soup.text;
            childs = {}
            if isinstance(arr[1],str):
                result[name] = {
                    'link': arr[1],
                    'type': arr[2],
                    'base': arr[3],
                    'other':arr[4]
                }
            else:
                for item  in arr[1]:
                    childs.update(self.makeNodes(item))
                result[name] = {
                    'child':childs,
                    'link': arr[2],
                    'type': arr[3],
                    'base': arr[4],
                    'other':arr[5]
                }
        elif len(arr) == 5:
            name = arr[0]
            soup = BeautifulSoup(name,"lxml");
            name = soup.text;
            childs = {}
            if isinstance(arr[1],str):
                result[name] = {
                    'link': arr[1],
                    'type': arr[2],
                    'base': arr[3],
                    'other':arr[4]
                }
            else:
                for item  in arr[1]:
                    childs.update(self.makeNodes(item))
                result[name] = {
                    'child':childs,
                    'link': arr[2],
                    'type': arr[3],
                    'base': arr[4]
                }
        elif len(arr) == 4:
            name = arr[0]
            soup = BeautifulSoup(name,"lxml");
            name = soup.text;
            childs = {}
            if isinstance(arr[1],str):
                result[name] = {
                    'link': arr[1],
                    'type': arr[2],
                    'base': arr[3]
                }
            else:
                for item  in arr[1]:
                    childs.update(self.makeNodes(item))
                result[name] = {
                    'child':childs,
                    'link': arr[2],
                    'type': arr[3]
                }
        elif len(arr) == 3:
            name = arr[0]
            soup = BeautifulSoup(name,"lxml");
            name = soup.text;
            result[name] = {
                "link": arr[1],
                "type": arr[2]
            }
        else:
            self.logger.info(arr);

        return result;

    def getNodeCode(self,names):
        node_ret = {}
        nodes = self.nodes;
        for name in names:
            for node in nodes:
                if node == name:
                    node_ret = nodes[node]
                    if 'child' in node_ret.keys():
                        nodes = node_ret['child']
        return node_ret['type']

    def resolveTree(self,dd):
        if len(dd.contents) == 2:
            name = dd.contents[1].text;
        elif len(dd.contents) == 1:
            name= dd.a.text;
        else:
            return {};

        div = dd.find('div')
        childs = {}
        if div != None:
            for dd in div.dl.children:
                childs.update(self.resolveTree(dd));
                pass
        return {name:childs};

    def resolveTreeArray(self,data,headers,trees):
        if headers == None:
            headers = []
        if trees == None:
            trees = []
        if len(data.keys()) > 0:
            for i in data.keys():
                headers.append(i)
                self.resolveTreeArray(data[i],headers,trees);
                headers.pop();
        else:
            trees.append(list(headers))

        return trees;

    def resolveNavTree(self,data):
        soup = BeautifulSoup(data,"lxml");
        tree = soup.find('div',class_='navtree');

        trees = []
        lstH3 = None
        for node in tree:
            if node.name == 'ul':
                name = lstH3.a.text
                
                childs = {}
                lstH3 = None;
                for li in node.children:
                    # self.logger.info(li)
                    childs.update(self.resolveTree(li));

                treeArray = self.resolveTreeArray(childs,[],[]);
                if len(treeArray) > 0:
                    for i in treeArray:
                        trees.append([name] + i)
                else:
                    trees.append([name])

            elif node.name == 'h3':
                if lstH3 != None:
                    name = lstH3.a.text
                    trees.append([name])
                lstH3 = node
        return trees;

    def parseNodes(self,response):
        pass
        data = response.body.decode('gb2312');
        # self.logger.info(data);
        # fp = open("tree.txt",'w')
        # fp.write(data);
        # fp.close();

        replaceData = re.sub(r"\\'", r"'", data);
        js = json.loads(replaceData) 
        self.nodes = self.makeNodes(js);

        # data = json.dumps(self.nodes,ensure_ascii=False);
        # self.logger.info(data);
        # fp = open("tree2.txt",'w')
        # fp.write(data);
        # fp.close();

        fp = open("navtree.txt",'r')
        data = fp.read();
        fp.close();
        # self.logger.info(data);
        
        trees = self.resolveNavTree(data)
        for tree in trees:
            code = self.getNodeCode(['行情中心'] + tree)
            # if code != 'gqg_hkstock_volume':
            #     continue;
            # self.logger.info(code)
            param = self.getCount_request(code,tree);
            # self.logger.info(param)
            yield scrapy.Request(param['url'],meta=param['meta'],callback= self.parseCount)
            break

    def parseCount(self,response):
        pass
        data = response.body.decode('gb2312');
        if data == "null":
            return;
        count = data[13:][:-3];
        if count.strip()=='' or len(count) == 0:
            return;
        else:
            pass
        tag = response.meta['tag']
        node = response.meta['node']
        param = self.getData_request(node,tag,1,int(count))
        yield scrapy.Request(param['url'],meta=param['meta'],callback= self.parseData)

    def parseData(self, response):
        pass
        self.logger.info(response.url)
        data = response.body.decode('gb2312');
        data = demjson.decode(data);
        self.logger.info(len(data));
        
        # {symbol:"sz300711",code:"300711",name:"广哈通信",
        # trade:"19.400",pricechange:"0.210",changepercent:"1.094",
        # buy:"19.390",sell:"19.400",settlement:"19.190",open:"19.190",
        # high:"19.520",low:"18.740",volume:2857915,amount:54821946,
        #ticktime:"15:00:03", 
        #per:40.417,
        #pb:4.974,mktcap:279740.15076,nmc:88562.94,turnoverratio:6.26036}
        
        #symbol:代码
        #code:编号
        #name:简称
        #trade：最新价
        #pricechange:涨跌额
        #changepercent:涨跌幅
        #buy:买入
        #sell:卖出
        #settlement:昨收
        #open:开盘
        #high:最高
        #low:最低
        #volume:成交量
        #amount:成交额
        #mktcap:总市值
        #nmc:流通市值
        #ticktime:时间
        #pb:市净率
        #turnoverratio:换手率

        for each in data:
            item = {};
            item = each;
            item['date'] = getLastClosingDate();
            item['type'] = 'DayClosingData'
            # yield item;

            #实时数据
            # yield scrapy.Request('http://hq.sinajs.cn/list='+ item['symbol'],meta=item,callback= self.parseNewData)
            
            code = item['code']
            symbol = item['symbol']
            #5分钟数据
            meta = {
            'symbol':symbol, #代码
            'scale':'5',    #分钟间隔 5,15,30,60
            'ma':'5',       #均值（5、10、15、20、25）
            'count':'1023'  #数量
            }
            url = Template('http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol=${symbol}&scale=${scale}&ma=${ma}&datalen=${count}')
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseMinuteData)
            
            #历史交易
            meta = {
            'symbol':symbol, #代码
            'code':code, #代码
            'year':'2018',
            'quarter':'1',       #季度 1 2 3 4
            }
            url = Template('http://money.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/${code}.phtml?year=${year}&jidu=${quarter}')
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseQuarterData)

            #历史交易明细数据接口,需要获取开盘日
            meta = {
            'symbol':symbol, #代码
            'date':time.strftime("%Y-%m-%d",time.localtime(time.time())), #代码
            'page':0,
            }
            url = Template('http://market.finance.sina.com.cn/transHis.php?symbol=${symbol}&date=${date}&page=${page}')
            yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseDetailsData)

            #资金流
            url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssi_ssfx_flzjtj?format=text&daima=" + symbol
            # yield scrapy.Request(url,meta=meta,callback= self.parseCapitalFlow)
            
            #https://blog.csdn.net/woloqun/article/details/80734088
            #财报数据
            url = Template("http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_FinanceSummary/stockid/${code}.phtml?qq-pf-to=pcqq.c2c")
            meta = {
            'code':code,
            'symbol':symbol
            }
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseFinanceSummaryData)
            
            #https://blog.csdn.net/luanpeng825485697/article/details/78442062?locationNum=5&fps=1
            #腾讯股票数据
            #分时图
            url = Template('http://data.gtimg.cn/flashdata/hushen/minute/${symbol}.js?maxage=${maxage}&${random}')
            meta = {
                'symbol' :symbol,
                'maxage':'110',
                'random':random.random()
            }
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseTencentMinuteData)
            
            #5天分时图
            url = Template('http://data.gtimg.cn/flashdata/hushen/4day/${tag}/${symbol}.js?maxage=${maxage}&visitDstTime=${visitDstTime}')
            meta = {
                'symbol' :symbol,
                'tag':symbol[0:2],
                'maxage':'110',
                'visitDstTime':1
            }
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseTencentDayData)
            
            #日k
            url = Template('http://data.gtimg.cn/flashdata/hushen/latest/daily/${symbol}.js?maxage=${maxage}&visitDstTime=${visitDstTime}')
            meta = {
                'symbol' :symbol,
                'maxage':'43201',
                'visitDstTime':1
            }
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseTencentDayKData)
            
            #年日K
            url = Template('http://data.gtimg.cn/flashdata/hushen/daily/${year}/${symbol}.js?visitDstTime=${visitDstTime}')
            meta = {
                'symbol' :symbol,
                'year':'2017'[-2:],
                'visitDstTime':1
            }
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseTencentYearDayKData)
            
            #周K
            url = Template('http://data.gtimg.cn/flashdata/hushen/latest/weekly/${symbol}.js?maxage=${maxage}&visitDstTime=${visitDstTime}')
            meta = {
                'symbol' :symbol,
                'maxage':'43201',
                'visitDstTime':1
            }
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseTencentWeekKData)
            
            #月K
            url = Template('http://data.gtimg.cn/flashdata/hushen/monthly/${symbol}.js?maxage=${maxage}')
            meta = {
                'symbol' :symbol,
                'maxage':'43201',
                'visitDstTime':1
            }
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseTencentMonthKData)
            
            #成交明细 列项
            url = Template('http://stock.gtimg.cn/data/index.php?appn=detail&action=timeline&c=${symbol}')
            meta = {
                'symbol' :symbol
            }
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseTencentClosingDetailsListData)
            
            #成交明细
            url = Template('http://stock.gtimg.cn/data/index.php?appn=detail&action=data&c=${symbol}&p=${page}')
            meta = {
                'symbol' :symbol,
                'page':0,
                'date':'20180413'
            }
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseTencentClosingDetailsData)
            
            #大单数据
            #opt=10 11 12 13 分别对应成交额大于等于（100万 200万 500万 1000万）
            #opt=1,2,3,4,5,6,7,8 分别对应成交量大于等于（100手 200手 300手 400手 500手 800手 1000手 1500手 2000手）
            url = Template('http://stock.finance.qq.com/sstock/list/view/dadan.php?t=js&c=${symbol}&max=${max}&p=${page}&opt=${opt}&o=${o}')
            meta = {
                'symbol' :symbol,
                'max':80,
                'page':0,
                'opt':10,
                'o':0,
            }
            # yield scrapy.Request(url.substitute(meta),meta=meta,callback= self.parseTencentLargeSingleData)
            
            break;

        #data[0]
        #https://hq.sinajs.cn/?_=1554047924366&list=ml_sh600100
        #公告
        #https://vip.stock.finance.sina.com.cn/api/jsonp.php/var%20noticeData=/CB_AllService.getMemordlistbysymbol?num=8&PaperCode=600100
        #ttps://news.sinajs.cn/rn=1554047925361&maxcnt=20&scnt=20&list=sh600100,gg_sh600100,ntc_sh600100,blog_sh600100,tg_sh600100,lcs_sh600100

        #ttps://vip.stock.finance.sina.com.cn/quotes_service/api/jsonp.php/var%20moneyFlowData=/MoneyFlow.ssi_ssfx_flzjtj?daima=sh600100&gettime=1
        
        #https://finance.sina.com.cn/realstock/company/sh600100/hisdata/klc_kl.js?d=2019_4_1
        return;

        node = response.meta['node']
        tag = response.meta['tag']
        count = int(response.meta['count'])
        page = int(response.meta['page'])
        if page * 80 < count:
            param = self.getData_request(node,tag,page + 1,count)
            yield scrapy.Request(param['url'],meta=param['meta'],callback= self.parseData)
        
    def parseNewData(self,response):
        pass;
        self.logger.info(response.body);

        symbol = response.meta['symbol']
        code = response.meta['code']
        data = response.body.decode('gb2312');
        index = data.find('\"')
        data = data[index:-1][0:-2];
        each = data.split(',')
        item = {}
        item['type'] = 'real'
        item['symbol'] = symbol
        item['code'] = code
        item['name'] = each[0];
        item['open'] = each[1];
        item['settlement'] = each[2];
        item['trade'] = each[3];
        item['high'] = each[4];
        item['low'] = each[5];
        # item[''] = each[6];    #买一
        # item[''] = each[7];    #卖一
        item['volume'] = each[8];
        item['amount'] = each[9];
        item['buys'] = each[10:19];
        item['sells'] = each[20:29];
        item['date'] = each[30];
        item['time'] = each[31];

        yield item;

    def parseMinuteData(self,response):
        pass
        symbol = response.meta['symbol']
        scale = response.meta['scale']
        data = response.body.decode('gb2312')
        js = demjson.decode(data);

        for  each in js:
            item = {}
            item['type'] = 'minute_' + str(scale)
            item['symbol'] = symbol
            item['date']= each['day'];
            item['open']= each['open'];
            item['high']= each['high'];
            item['low']= each['low'];
            item['close']= each['close'];
            item['volume']= each['volume'];

            yield item

    def parseQuarterData(self,response):
        pass
        symbol = response.meta['symbol']
        self.logger.info(response.url);
        data = response.body.decode('gb2312')
        data = data.replace('\r','').replace('\n','').replace('\t','')
        # self.logger.info(data);
        soup = BeautifulSoup(data,"lxml");
        table = soup.find("table",id='FundHoldSharesTable');
        tds = table.find_all('td');
        # self.logger.info(tds);
        for i in range(0,len(tds),7):
            if i == 0:
                continue;
            item = {}
            item['type'] = 'quarter'
            item['symbol'] = symbol
            item['date']= tds[i+0].get_text();
            item['open']= tds[i+1].get_text();
            item['high']= tds[i+2].get_text();
            item['low']= tds[i+3].get_text();
            item['close']= tds[i+4].get_text();
            item['volume']= tds[i+5].get_text();
            item['amount']= tds[i+6].get_text();
            yield item;

    def parseDetailsData(self,response):
        pass
        symbol = response.meta['symbol']
        self.logger.info(response.url);
        data = response.body.decode('gb2312')
        # data = data.replace('\r','').replace('\n','').replace('\t','')
        self.logger.info(data);
        soup = BeautifulSoup(data,"lxml");
        table = soup.find(table,class_='datatbl');
        if table == None:
            return;
        trs = table.find_all('tr');
        index = 0;
        for tr in trs:
            index+=1;
            if index == 1:
                continue;

            item = {}
            item['type'] = 'TencentClosingDetails'
            item['symbol'] = symbol
            item['date'] = date
            item['time'] = tr.contents[0].get_text();
            item['trade'] = tr.contents[1].get_text();
            item['pricechange'] = tr.contents[2].get_text();
            item['volume'] = tr.contents[3].get_text();
            item['amount'] = tr.contents[4].get_text();
            nature = tr.contents[5].get_text();
            item['nature'] = nature
            yield item;
            

    def parseCapitalFlow(self,response):
        symbol = response.meta['symbol']

        data = response.body.decode('gb2312')
        data = data[1:-1]
        self.logger.info(data);
        js = demjson.decode(data);

        item = js;
        item['type'] = 'capitalflow'
        item['symbol'] = symbol;
        item['date'] = time.strftime("%Y%m%d %H:%M:%S", time.localtime(time.time()))

        yield item;
    
    #财务摘要
    def parseFinanceSummaryData(self,response):
        symbol = response.meta['symbol']
        self.logger.info(response.url);
        data = response.body.decode('gb2312')
        # self.logger.info(data);
        soup = BeautifulSoup(data,"lxml");
        table = soup.find("table",id='FundHoldSharesTable');
        tds = table.find_all('td');
        item = {}
        item['type'] = 'financesummary'
        item['symbol'] = symbol
        item['data'] = []
        name = ''
        for td in tds:
            pass
            if td.get_text() == '截止日期':
                yield item
                item = {}
                item['type'] = 'financesummary'
                item['symbol'] = symbol
                item['data'] = []
            if name == '':
                name = td.get_text().replace('-','');
            else:
                value = td.get_text()
                self.logger.info(value);
                if value == '\xa0':
                    value = ''

                if name == '截止日期':
                    item['date'] = td.get_text()
                item['data'].append({'name':name,'value':value})
                name = ''

        if len(item['data']) > 0:
            yield item

    #腾讯数据
    def parseTencentMinuteData(self,response):
        pass
        symbol = response.meta['symbol']
        self.logger.info(response.url);
        data = response.body.decode('gb2312')
        data = data[10:-2]
        strs = data.split('\\n\\\n');
        # self.logger.info(strs);

        item = {}
        item['type'] = 'TencentMinute'
        item['symbol'] = symbol
        item['data'] = []
        for each in strs:
            if each == '':
                continue;
            its = each.split(' ')
            if len(its) == 1:
                ds = each.split(':')
                item['date'] = '20' + ds[1];
            else:
                item['data'].append(its);
        yield item;

    def parseTencentDayData(self,response):
        pass
        symbol = response.meta['symbol']
        # self.logger.info(response.url);
        data = response.body.decode('gb2312')
        data = data[16:-1]
        js = demjson.decode(data);
        for each in js:
            # self.logger.info(each);
            item = {}
            item['type'] = 'TencentDay'
            item['symbol'] = symbol
            item['date'] = each['date']
            item['prec'] = each['prec']
            item['data'] = []
            its = each['data'].split('^');
            for it in its:
                lts = it.split('~')
                item['data'].append(its);
            yield item;

    def parseTencentDayKData(self,response):
        pass
        symbol = response.meta['symbol']
        # self.logger.info(response.url);
        data = response.body.decode('gb2312')
        data = data[19:-2]
        strs = data.split('\\n\\\n');
        for each in strs:
            if each == '':
                continue;
            its = each.split(' ')
            if len(its) > 6:
                ds = each.split(':')
                #暂时不清楚意思
                #100 total:3871 start:021009 02:59 03:239 04:241 05:241 06:217 07:240 08:238 09:241 10:240 11:243 12:242 13:238 14:245 15:244 16:244 17:149 18:243 19:67
            else:
                item={}
                item['symbol'] = symbol
                item['type'] = 'TencentDayK'
                item['date'] = '20' + its[0];
                # 昨 最新 最高 最低 成交量
                item['settlement'] = its[1]
                item['trade'] = its[2]
                item['high'] = its[3]
                item['low'] = its[4]
                item['volume'] = its[5]
                yield item;

    def parseTencentYearDayKData(self,response):
        pass
        symbol = response.meta['symbol']
        # self.logger.info(response.url);
        data = response.body.decode('gb2312')

        data = data[15:-2]
        strs = data.split('\\n\\\n');
        # self.logger.info(strs);
        for each in strs:
            if each == '':
                continue;
            its = each.split(' ')

            item={}
            item['symbol'] = symbol
            item['type'] = 'TencentYearDayK'
            item['date'] = '20' + its[0];
            # 昨 最新 最高 最低 成交量
            item['settlement'] = its[1]
            item['trade'] = its[2]
            item['high'] = its[3]
            item['low'] = its[4]
            item['volume'] = its[5]
            yield item;

    def parseTencentWeekKData(self,response):
        pass
        symbol = response.meta['symbol']
        # self.logger.info(response.url);
        data = response.body.decode('gb2312')

        data = data[20:-2]
        strs = data.split('\\n\\\n');
        for each in strs:
            if each == '':
                continue;
            its = each.split(' ')
            if len(its) != 6:
                self.logger.info(each)
                ds = re.split('[ :]',each);
                self.logger.info(ds)
            else:
                item={}
                item['symbol'] = symbol
                item['type'] = 'TencentWeekK'
                item['date'] = '20' + its[0];
                # 昨 最新 最高 最低 成交量
                item['settlement'] = its[1]
                item['trade'] = its[2]
                item['high'] = its[3]
                item['low'] = its[4]
                item['volume'] = its[5]
                yield item;

    def parseTencentMonthKData(self,response):
        pass
        symbol = response.meta['symbol']
        self.logger.info(response.url);
        data = response.body.decode('gb2312')

        data = data[14:-2]
        strs = data.split('\\n\\\n');

        for each in strs:
            if each == '':
                continue;
            its = each.split(' ')
            item={}
            item['symbol'] = symbol
            item['type'] = 'TencentMonthK'
            item['date'] = '20' + its[0];
            # 昨 最新 最高 最低 成交量
            item['settlement'] = its[1]
            item['trade'] = its[2]
            item['high'] = its[3]
            item['low'] = its[4]
            item['volume'] = its[5]
            yield item

    def parseTencentClosingDetailsListData(self,response):
        pass
        symbol = response.meta['symbol']
        self.logger.info(response.url);
        data = response.body.decode('gb2312')
        data = data[15+len(str(symbol)):];
        js = demjson.decode(data)
        data = re.split('[|]',js[1])
        self.logger.info(js[0])
        self.logger.info(len(data));

    def parseTencentClosingDetailsData(self,response):
        pass
        symbol = response.meta['symbol']
        date = response.meta['date']
        self.logger.info(response.url);
        data = response.body.decode('gb2312')
        data = data[15+len(str(symbol)):];
        js = demjson.decode(data)
        data = re.split('[/_]',js[1])

        for i in range(0,len(data),7):
            item = {}
            item['type'] = 'TencentClosingDetails'
            item['symbol'] = symbol
            item['date'] = date
            item['time'] = data[i+1];
            item['index'] = data[i];
            item['trade'] = data[i+2];
            item['pricechange'] = data[i+3];
            item['volume'] = data[i+4];
            item['amount'] = data[i+5];
            if data[i+6] == 'S':
                item['nature'] = '卖盘';
            elif data[i+6] == 'B':
                item['nature'] = '买盘';
            else:
                self.logger.warn(data[i+6]);
            yield item;

    def parseTencentLargeSingleData(self,response):
        pass
        symbol = response.meta['symbol']
        self.logger.info(response.url);
        data = response.body.decode('gb2312')
        data = data[15+len(str(symbol)):];