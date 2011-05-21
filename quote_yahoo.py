#!/usr/bin/env python
# -*- coding:  utf-8 -*-
"""fetch realtime and history stock data from yahoo 

"""
# ============================================================================
# detailed description
# ============================================================================
# 
# 
# ============================================================================
# imports
# ============================================================================
import urllib2
import os
import os.path
from stat import *
import time
import sys
import datetime as dt
#import pdb
#import parser
import json
import random

import numpy as np
import tables as tb
#from matplotlib.dates import date2num,num2date


from tools import  market_time_series

__version__ = "$revision: 0.1_20101224$"
__status__ = "prototype"

# ============================================================================
# defination
# ============================================================================

# these times should be set to minimize the network traffic
QUOTEEXPIRY = 5 # 5 seconds

YAHOO_WEB_BASE = 'http://w01.znz.finance.bj1.aliyk.com'

# data servers of yahoo realtime stock quote
#    'data01.znz.finance.bj1.aliyk.com',
#    'data02.znz.finance.bj1.aliyk.com',
#    'data03.znz.finance.bj1.aliyk.com',
#    'data04.znz.finance.bj1.aliyk.com',
#    'data05.znz.finance.bj1.aliyk.com'
YAHOO_CGI_HEAD = 'http://data0{0}.znz.finance.bj1.aliyk.com/test/'
YAHOO_CGI_TAIL = '&from=finance.cn.yahoo.com HTTP/1.1'
KMIN_DAY = 'kmin.py/data.znzDo?cmd={1}|{2}|{3}|{4}|{5}'
RT = 'data.py/quick?cmd={1}{2}|{3}|{4}|{5}|{6}'
KMIN_HIST = 'kmin.py/wmData.znzDo?cmd={1}|{2}|{3}'
YAHOO_RT = ''.join([YAHOO_CGI_HEAD,RT,YAHOO_CGI_TAIL])
YAHOO_KMIN_DAY = ''.join([YAHOO_CGI_HEAD,KMIN_DAY,YAHOO_CGI_TAIL]) 
YAHOO_KMIN_HIST = ''.join([YAHOO_CGI_HEAD,KMIN_HIST,YAHOO_CGI_TAIL]) 

YAHOO_DIVID = 'http://finance.cn.yahoo.com/mirror/F10/{0}_d_6.html?r={1}'


# ============================================================================
# Tools
# ============================================================================

# ============================================================================
# Quoter
# ============================================================================
class Quoter(object):
    """
    1. get watch list
    3. check connection
    2. get quotes from rt source
    3. scan data and triger signal
    4. emit signal
    """
    def __init__(self,code=None,fpath=None):
        """
        init:
        1) contruct stocklist
        2) create/open txtfile and hd5filr
        todo:
        1)code format check - regx
        @codelist: stocks to be scanned, strock ['sh600001',sz000001',...]        
        """
        self.m_code= code if code else 'SH510050' 
        
        
    def fetch_quote_rt(self, stock, 
                        time_info=None,snumber=0,parse=True):
        """ fetch rt quote  

         query_url_head = 
           http://data0{0-5}.znz.finance.bj1.aliyk.com/test/data.py/quick
         query_url =Query_url_head
               + '?cmd='  
               + this.mStockType.toLowerCase()
               + this.mStockCode + '|'  
               + this.timeInfo[0] + '|'  
               + this.timeInfo[1] + '|'  
               + this.sNumber + '|'  
               + Math.random().toString()

         Raw quote data returned 
         {"info":{ 
           "index":false,
           "sNumber":3037,
           "instant":[1.896,        -> prev close
                      1.893,        -> open (day open)
                      1.900,        -> last trade 
                      2017585,      -> volume(x100 shares)
                      38431.63,     -> total(x10000 RMB)
                      1.915,        -> high (day high)
                      1.891,        -> low (day low)
                      1.900,7088,   -> bid#1 price, size (x100 share)
                      ....,
                      1.896,7897,   -> bid#5 price, size (x100 share)
                      1.901,550,    -> ask#1 price, size (x100 share)
                      ....,
                      1.906,3726],  -> ask#5 price, size (x100 share) 
           "items": [3010,     -> 10 trading details before queried time 
                     3036,
                     [['112839', 1.905, 12668,'B'],  -> [time,tradepice,size,type]
                     ['112854', 1.905, 1009, 'B'],   -> here, size: x100 share  
                     ['112859', 1.905, 100,  'B'],   -> typr: B for buy, S for sale
                     ['112904', 1.905, 145,  'B'],
                     ['112909', 1.902, 10000,'S'],
                     ['112929', 1.902, 8779, 'S'],
                     ['112934', 1.902, 350,  'B'],
                     ['112939', 1.902, 50,   'B'],
                     ['112944', 1.901, 10000,'S'],
                     ['112954', 1.900, 10100,'S']]],
                "longPrice":true,
                "start":0, -> index of minute, 9:30->1,9:31->2...14:58->239,14:59->240  
                "timeInfo":["20100616","113019"], -> query date and time
                "in":2171160.0,                  -> neipan
                "history":         
                         [[1.893, 4035, 76.39],  -> start from query time, per minute
                         [1.897, 6382, 120.86],  -> if time is 000000, from 0930,
                         [1.897, 6382, 120.86],  -> [tradeprice,volume,total]
                         [1.897, 8513, 161.25],  -> here, volume is cumulated   
                         [1.896, 16450,311.69],
                         ...
                         [1.900,2017585,38431.64]],
                 "out":919537.0}                 -> waipan
          "cmd":"QUICK_GET",
          "ret":"OK"
         }0
         
        Parameters
        ----------
        @stock: SH510050
        @time_info: '20100611-145812', if '000000-000000' will return 
                    with full history data 
        @snumber: index of data record in server side, 0 ~ 6861(depends).
        @parse: default True to parse data

        Returns
        -------
        result: dict {'data': ..., 'url':..} 
            where data parsed in JSON see above
        """
        if time_info is None:
            time_info = time.strftime('%Y%m%d-%H%M%S').split('-')
        else:
            time_info = time_info.split('-')

        url = YAHOO_RT.format(
                random.randint(1,5), stock[:2].lower(),
                stock[2:], time_info[0], time_info[1], 
                snumber, random.random())
        
        result = {'data':None,'url':url}
        #data = self.download(url)
        try:
            fp = urllib2.urlopen(url)
            raw_data = fp.read()
            fp.close()
            if parse:
                data = json.loads(raw_data.rstrip('0'))
                #print data
                result['data'] = data
            else:
               result['data'] = raw_data
            #return result
        except urllib2.URLError as e:
            if hasattr(e, 'reason'):
                print 'Failed to reach the server: ', e.reason
            elif hasattr(e, 'code'):
                print 'Error request from server (Error Code): ', e.code
            #return result
        except  (TypeError, ValueError):
            # Json error
            print 'Failed to parse fetched data: ', raw_data
            #return result
        except: 
            print "\nUnexpected error:", sys.exc_info()[0]
            raise
        return result

    def fetch_quote_km(self, stock,
                        kmin='1',snumber=0,parse=True):
        """ fetch today's minute data
                
        query_url_head = 
          http://data05.znz.finance.bj1.aliyk.com/test/kmin.py/data.znzdo
        query_url = query_url_head 
          + '?cmd='
          + 'sh510050' + '|'      -> code
          + '5' + '|'             -> 1,5,15,60 for 1/5/15/60 minute data 
          + '20101208' +'|'       -> today 
          + '47' + '|'            -> order of the minutes of the day, 
                                  -> 0~47 for 5 min, 0~239 for 1min, ...
                                  -> 0 for all availabel data till the query moment
                                  -> seems no impact to return
          + '0.41996181244030595' -> random number
        
        raw minute data returned 
        '[20101210,       -> date of query
          true,           -> falg
          0,              -> number of minute, 0 for today's all available data
          "[[1.981,1.984,1.981,1.984,38929.00,771.38],
            [1.983,1.989,1.983,1.989,65139.00,1291.56],
            [open, high,low,close,vol(x100),amount(x10000)]
            ...
            [2.010,2.010,2.009,2.009,3177184.00,63551.16]]"
         ]0'

        Parameters
        ----------
        @stock: SH510050
        @kmin: kindle of minute, '5/15/30/60' minute.  
        @parse: default True to parse data

        Returns
        -------
        result: dict{'data': ..., 'url':query url}
            where: data = {'sNUmber': quote list}
        """
        code = stock.lower()
        date = time.strftime('%Y%m%d')
        url = YAHOO_KMIN_DAY.format(
                random.randint(1,5), code, kmin,
                    date, snumber, random.random())
         
        result = {'data':None,'url':url}
        try:
            fp = urllib2.urlopen(url)
            raw_data=fp.read()
            fp.close()
            if parse:
                data = {}
                tmp_d = json.loads(raw_data.rstrip('0'))
                data.update({
                    str(tmp_d[2]): json.loads(tmp_d[3])
                    })
                #print data
                result['data'] = data
            else:
                result['data'] = raw_data
        except urllib2.URLError as e:
            if hasattr(e, 'reason'):
                print 'Failed to reach the server: ', e.reason
            elif hasattr(e, 'code'):
                print 'Error request from server (Error Code): ', e.code
            #return None
        except  (TypeError, ValueError, IndexError):
            # Json error
            print 'Failed to parse fetched data: ', raw_data
            #return None
        except: 
            print "\nUnexpected error:", sys.exc_info()[0]
            raise
        return result

    def fetch_hist_km(self, stock, kmin='1',parse=True):
        """ fetch history minute data

        query_url_head = 
          http://data05.znz.finance.bj1.aliyk.com/test/kmin.py/wmdata.znzdo
        query_url = query_url_head 
          + '?cmd='
          + '1' + '|'             -> 1,5,15,60 for 1/5/15/60 minute dat 
          + 'shhq510050' + '|'      -> code
          + '0.41996181244030595' -> random number

        raw history minute data returned 
        '[[20101206, "[[1.981,1.984,1.981,1.984,38929.00,771.38],
                       ...
                       [1.981,1.984,1.981,1.984,38929.00,771.38]]"], 
          [20101207, "[[1.981,1.984,1.981,1.984,38929.00,771.38],
                       ...
                       [1.981,1.984,1.981,1.984,38929.00,771.38]]"],  
          [20101208,   ...                                        ],
          [20101209, "[[1.981,1.984,1.981,1.984,38929.00,771.38],
                       ...
                       [1.981,1.984,1.981,1.984,38929.00,771.38]]"]]0'
        Parameters
        ----------
        @stock: SH510050
        @kmin: kindle of minute, '5/15/30/60' minute.  
        @parse: default True to parse data
        
        Returns
        -------
        result: dict {'data': ..., 'url':query url}
            where: data = {'20101207':quote list, '20101208':quote list, ...}
        """
        code = stock[:2].upper()+'HQ'+stock[2:]
        url = YAHOO_KMIN_HIST.format( 
                random.randint(1,5), kmin,code,random.random())

        result = {'data':None,'url':url}
        try:
            fp = urllib2.urlopen(url)
            raw_data=fp.read()
            fp.close()
            if parse:
                data ={}
                tmp_d= json.loads(raw_data.rstrip('0'))
                for x in tmp_d:
                    data[str(x[0])] = json.loads(x[1])
                #print data
                result['data'] = data 
            else:
                result['data'] = raw_data
        except urllib2.URLError as e:
            if hasattr(e, 'reason'):
                print 'Failed to reach the server: ', e.reason
            elif hasattr(e, 'code'):
                print 'Error request from server (Error Code): ', e.code
            #return None
        except  (TypeError, ValueError, IndexError):
            # Json error
            print 'Failed to parse fetched data: ', raw_data
            #return None
        except: 
            print "\nUnexpected error:", sys.exc_info()[0]
            raise
        return result
    
    def fetch_divid(self,stock,parse=True):
        """fetch all dividend data of a stock

        query_url_head = 
          http://finance.cn.yahoo.com/mirror/F10/
        query = query_url_head 
          + 'shhq510050'              -> code
          + '_d_6.html?'              -> fixed string for dividend
          + 'r=0.37872775095295896'   -> random number  

        raw minute data returned 
        HTLM data (To be added)

        Parameters
        ----------
        @stock: SH510050
        @parse: default True to parse data

        Returns
        -------
        result: dict {'data': ..., 'url':..} 
            where data parsed in JSON see above
        """
        code = stock[:2].upper()+'HQ'+stock[2:]
        url = YAHOO_DIVID.format(code,random.random())
        result = {'data':None,'url':url}
        try:
            fp = urllib2.urlopen(url)
            raw_data=fp.read()
            fp.close()
            if parse:
                data = raw_data
                #print data
                result['data'] = raw_data 
            else:
                result['data'] = raw_data
        except urllib2.URLError as e:
            if hasattr(e, 'reason'):
                print 'Failed to reach the server: ', e.reason
            elif hasattr(e, 'code'):
                print 'Error request from server (Error Code): ', e.code
            #return None
        except  (TypeError, ValueError, IndexError):
            # Json error
            print 'Failed to parse fetched data: ', raw_data
            #return None
        except: 
            print "\nUnexpected error:", sys.exc_info()[0]
            raise
        return result
    
    def km2cvs(self,km_obj,fmt='',title=True,sep=','):
        """convert minute data to cvs format
        
        Parameters
        ----------
        @km_objk: kmin_obj as {'data':...,'url':...}
        where, data ={'20101207':quote_list,'20101208':quote_list, ...}
               quote_list=[open,high,low,close,vol(x100),amount(x10000)]
        @fmt:   
        @title: default True to insert title
            'date,time,open,high,low,close,vol(x100),amount(x10000)'
            date:20101207, time:0935
        @sep: deliminate between fields, default ',' 
        
        Returns
        -------
        result: string in cvs format like below
            date,time,open,high,low,close,vol(x100),amount(x10000)
            20101207,0935,1.981,1.984,1.981,1.984,38929.00,771.38
        """
        tmp_title =sep.join(['date','time','open','high','low','close',
                            'vol(x100)','amount(x10000)'])
        # check input obj is history km data or day km 
            
        km_fmt =sep.join(['{0:.3f}','{1:.3f}',
                          '{2:.3f}','{3:.3f}',
                          '{4:.0f}','{5:.3f}'])
        try:
            tmp_km_type = km_obj['url'].split('=')[-1].split('|') 
        except:
            print 'Invaled input data for csv convertion'
            return None
        
        if len(tmp_km_type) == 3:
            # history data
            flg_hist = True
            flg_min = tmp_km_type[0]
        elif len(tmp_km_type) == 5:
            # day minute data    
            flg_hist = False
            flg_min = tmp_km_type[1]            

        if km_obj['data'] is None:
            return None

        result=[]
        if flg_hist:
            # for history quote
            # here only hour and minute in time series 
            ts = market_time_series(step=flg_min,out_fmt='%H%M')
            day_lst = km_obj['data'].keys()
            day_lst.sort()  
            for tmp_day in day_lst:
                for i,d in enumerate(km_obj['data'][tmp_day]):
                    result.append(sep.join([tmp_day,ts[i],km_fmt.format(*d)]))
            return result    
        else:
            # for intraday quote 
            # here only hour and minute in time series 
            ts = market_time_series(step=flg_min,
                                    out_fmt=sep.join(['%Y%m%d','%H%M']))
            for i,d in enumerate(km_obj['data']['0']):
                result.append(sep.join([ts[i],km_fmt.format(*d)]))
            return result    

    def output_quote(self,data,outputfp=None,printout=True):
        """print quote to stdout
        @data: dictory-data to be print 
        """
        if printout: 
            if data is not None and data.get('ret') =='OK':
                quote = data['info']['instant']
                print('{0}: p-c:{1:.3f}, o:{2:.3f}, c:{3:.3f}, h:{4:.3f}, l:{5:.3f}'.format(
                    data['info']['timeInfo'][1],
                    quote[0],quote[1],quote[2],quote[5],quote[6]))
                print('Ask: {0:.3f}/{1}, {2:.3f}/{3}, {4:.3f}/{5}, {6:.3f}/{7}, {8:.3f}/{9}'.format(
                    quote[17],quote[18],quote[19],quote[20],quote[21],quote[22],
                    quote[23],quote[24],quote[25],quote[26]))
                print('Bid: {0:.3f}/{1}, {2:.3f}/{3}, {4:.3f}/{5}, {6:.3f}/{7}, {8:.3f}/{9}'.format(
                    quote[7],quote[8],quote[9],quote[10],quote[11],quote[12],
                    quote[13],quote[14],quote[15],quote[16]))
        
        if outputfp is not None:        
            outputfp.writelines(str(data)+'\n')
    

def main():
    pass

if __name__ == "__main__":
    main()            
