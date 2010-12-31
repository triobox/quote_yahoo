#!/usr/bin/env python
# -*- coding:  utf-8 -*-
"""parse fxj stock data file 

fxj daily and 5-minute stock data file ends with .DAD, 
Dividend and split information ends with .PWR, 
.FIN file is finance data file.  
"""
# ============================================================================
# imports
# ============================================================================

import sys
import struct
import datetime as dt
import numpy as np
import os
#from os.path 
from time import strptime

__version__ = "$revision: 0.1_20101224$"
__status__ = "prototype"

# ============================================================================
# defination
# ============================================================================

market={'SH':'SHA','SZ':'SHE','HK':'HKG'}

# time series [9:35, ..., 11:30,13:05,...15:00]
TimeSeries = np.array([34500, 34800, 35100, 35400, 35700, 36000, 36300, 36600, 36900,
            37200, 37500, 37800, 38100, 38400, 38700, 39000, 39300, 39600,
            39900, 40200, 40500, 40800, 41100, 41400, 47100, 47400, 47700,
            48000, 48300, 48600, 48900, 49200, 49500, 49800, 50100, 50400,
            50700, 51000, 51300, 51600, 51900, 52200, 52500, 52800, 53100,
            53400, 53700, 54000], dtype=np.uint32)


Maptable={
      'SH1A0001':'SH000001',
      'SH1A0002':'SH000002',
      'SH1A0003':'SH000003',
      'SH1B0001':'SH000004',
      'SH1B0002':'SH000005',
      'SH1B0004':'SH000006',
      'SH1B0005':'SH000007',
      'SH1B0006':'SH000008',
      'SH1B0007':'SH000010',
      'SH1B0008':'SH000011',
      'SH1B0009':'SH000012',
      'SH1B0010':'SH000013',
      'SH1C0002':'SH000015',
      'SH1C0003':'SH000016',
      'SH1C0004':'SH000017' }

QUOTE_DAY_TITLE =','.join(['date','open','high','low','close',
                            'vol','total'])
QUOTE_DAY_STR_FMT = ','.join(['{0}','{1:.3f}','{2:.3f}',
                              '{3:.3f}','{4:.3f}','{5:.0f}','{6:.3f}'])
QUOTE_MIN_TITLE =','.join(['time','open','high','low','close',
                            'vol','total'])
QUOTE_DAY_TITLE = QUOTE_MIN_TITLE
QUOTE_MIN_STR_FMT = QUOTE_DAY_STR_FMT

TOHLCVS_TITLE =','.join(['time','open','high','low','close', 'vol','sum'])
OHLCVS_TITLE =','.join(['open','high','low','close', 'vol','sum'])
# str.format for quote with time
TOHLCVS_STR_FMT = ','.join(['{0}','{1:.3f}','{2:.3f}',
                              '{3:.3f}','{4:.3f}','{5:.0f}','{6:.3f}'])
# str.format for quote with/0 time
OHLCVS_STR_FMT = ','.join(['{0:.3f}','{1:.3f}','{2:.3f}',
                              '{3:.3f}','{4:.0f}','{5:.3f}'])

TIME_NUM2STR_FMT = '%Y%m%d,%H%M' 
DATE_NUM2STR_FMT = '%Y%m%d'

TOHLCVS_STR_FMT _DAY_ARR_FMT = 'I4,f4,f4,f4,f4,f4,f4'
QUOTE_MIN_ARR_FMT = QUOTE_DAY_ARR_FMT
# ============================================================================
# tools
# ============================================================================

class myError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __repr__(self):
        return str(self.msg)
    def __str__(self):
        return str(self.msg)

def dt_num2str (num,fmt='%Y%m%d'):
    """ convert datetime int to datetime str
    @num is an int of second since 1970-01-01-0-0-0
    @fmt is str_fmt
    """
    d = dt.datetime(1970,1,1,0,0) + dt.timedelta(seconds=int(num))

    return d.strftime(fmt)

def readx2(file,fmt,position=None, number=1):
    """read a number of data from file 
    return a list of list according to given format, 
    """        
    if position == None: position = file.tell()
    file.seek(position)
    size = struct.calcsize(fmt)
    x= file.read(size*number)
    try:
        lst=[list(struct.unpack(fmt,x[i:i+size])) for i in range(0,size*number,size)]
    except:
        return []
    
    #if isarray: return np.rec.array(lst, names=name)    
    return lst

def quote2str(quote,time_fmt,out_fmt=OHLCVS_STR_FMT):
    """comvert quote list/array to list of str
    
    parameter
    --------
    @quote: list of quotes, like [[time,o,h,l,c,v,s],...] 
        where, time is an int based on 1970-1-1
    @time_fmt: fmt to convert time int to str, 
        where, date like'%Y%m%d', date+time like '%Y%m%d-%H%M'
    @out_fmt: string.format to apply to quote list w/o time, 
        default for OHLCVS list like this 
        '{0:.3f},{1:.3f},{2:.3f},{3:.3f},{4:.0f},{5:.3f}'
    
    return
    ------
    result: list of str, like['20100927-0930,2.13,,,,',...]
        where, time fmt depends on time_fmt
    """
    result = [','.join([dt_num2str(x[0],fmt=time_fmt),
                out_fmt.format(*x[1:])]) for x in quote]
    
    return result

# ============================================================================
# functions
# ============================================================================

def parse_pwr(fp,out_str=True):
    """ read dividend & split data (.PWR file)
    
    parameter
    ---------
    @fp: file point of pwr file
    @out_str: ouput string of divid data if True(default) 

    return
    ------
    a list = [divid_dict, err]
    where: divid_dict ={'SH500001':
                          ['20050623,0.000,0.000,0.000,0.125', 
                           '20051130,0.320,0.000,0.000,0.000', ...],
                         'SZ000200': ...}
    """
    # filetag_1:4B = /0xff43c832 or 4282632242L; 
    # filetag_2:4B = /0xffcc83dd or 4291593181L; 
    fmt_file_head='<II' 
    # record head 
    # DATA:4, 0xffffffff for rec head
    # symbol:8, market flag + code, like SH500001 
    # unknown:4
    fmt_rec_head = '<I8sII' 
    # record body
    # Date,
    # Sending /pershare(free), sending/pershare(charged), 
    # sending price, dividend
    fmt_rec_body = '<Iffff' 
    #rec_length = 32
    
    # count=[code numbe,failed number,except]
    count=[0,0,'']
    #  to be returned 
    result={}

    # read file head
    head = readx2(fp,fmt_file_head,position=0)[0]

    if head[0]!=4282632242 and head[1]!=4291593181:
        raise myError('Not a fxj PWR file')
    
    while 1: #eof?
        # read 4 byte after file head
        data = readx2(fp,'I')
        if len(data)==0:
            break
        # if date is 0xffffffff, it's a record head
        if data[0][0] == 0xffffffff:
            
            rec = readx2(fp,fmt_rec_head,position=fp.tell()- 4)[0]
            # check if code need to exchange
            tmp_code = rec[1].upper()
            if Maptable.has_key(tmp_code):
                tmp_code = Maptable[tmp_code] 
            result[tmp_code] = []
            count[0] += 1
            #tmp_1day_rec=[]
            #time_tag = None
            #tmp_index = tmp_index - 1
            #print tmp_index, tmp_code, rec[4]
        else:
            # else, it's a record with time 
            rec = readx2(fp,fmt_rec_body,
                          position=fp.tell()- 4)[0]
            if out_str:
                rec = '%s,%.3f,%.3f,%.3f,%.3f' %(
                            dt_num2str(int(rec[0])),
                            rec[1],rec[2],rec[3],rec[4])

            # append rec into dic 
            if result.has_key(tmp_code):            
                result[tmp_code].append(rec)
            else:
                count[1] += 1
                count[2] += ','+tmp_code
    err = '# Total stock %d, failed %s, error: %s' %tuple(count)
         
    return [result,err]

def parse_fin(fp,out_str=True):
    """ read finance data (.FIN file) 

    parameter
    ---------
    @fp: file point of fin file
    @out_str: ouput string of divid data if True(default) 

    return
    ------
    a list = [divid_dict, err]
    where: divid_dict ={'SH500001':
                          ['20050623,0.000,0.000,0.000,0.125', 
                           '20051130,0.320,0.000,0.000,0.000', ...],
                         'SZ000200': ...}
    """
    
    # filetag_1:4B  =/0x223fd90c (574609676)  
    # len of rec:4B =/0xa6 (166) 
    fmt_file_head='<II' 
    ### record body
    # market flag: 2s, 'SH' or 'SZ'
    # unkonwn: 2s, '\0x03\0x00' 
    # code: 6s
    # unknown: I 
    # date: I
    # data: f*37
    fmt_rec_body = '<2s2s6sII'+37*'f' 
    #rec_length = 32

    # read file head
    head = readx2(fp,fmt_file_head,position=0)[0]

    if head[0]!=574609676:
        raise myError('Not a fxj FIN file')

    rec_len = head[1] #166
    
    # count=[code numbe,failed number,except]
    count=[0,0,'']
    #  to be returned 
    result={}

    while 1:
        data = readx2(fp,fmt_rec_body)
        if len(data) == 0:
            break
        tmp_code = data[0][1]+data[0][2]
        if Maptable.has_key(tmp_code):
            tmp_code = Maptable[tmp_code] 
        if result.has_key(tmp_code):
            count[1] += 1
            count[2] = ','.join(count[2],tmp_code)
        else:    
            result[tmp_code] = data[0][4:]
        count[0] += 1

    err = '# Total stock %d, failed %s, error: %s' %tuple(count)
         
    return [result,err]

def parse_dad(fp, is_daily=True, out_str=True): 
    """ read daily data (.DAD file)
    
    parameter
    ---------
    @fp: file point of dad file
    @is_daily: input is daily (default) data or 5-min data
    @out_str: ouput string of divid data if True(default) 

    return
    ------
    a list = [daily_dict, err]
        where, daily_dict = {'SH000001':
                            ['date#1, open, high,low,close,volume,sum', 
                             'date#2, open, high,low,close,volume,sum', 
                             ...], 'SH600001': [...],...}
        quote inside is str, otherwise numpy array               
    err = str of 'Total stock num, failed num, failed code' 
    """
    # filetag:4 (33 FC 19 8C) = 872159628; unknown:4; record #:4; unknown4 
    fmt_file_head='<IIII' 
    #DATA:4, symbol:8, unknown:4, unknown:4, name:8, unknow:4
    fmt_rec_head = '<I8sII8sI' 
    #DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, SUM, unknow
    fmt_rec_data = '<IffffffI' 
    rec_length = 32
    
    if is_daily:
        rec_number_block = 1
        time_num2str_fmt = DATE_NUM2STR_FMT
    
    else:
        rec_number_block = 48  
        time_num2str_fmt = TIME_NUM2STR_FMT
    
    # count=[total number, failed number, failed code]
    count=[0,0,'']
    #  to be returned 
    dict_symbol={}
    try:
        # read file head
        head = readx2(fp,fmt_file_head,position=0)[0]
    
        if head[0]!=872159628:
            raise myError('Not a fxj DAD file')

        # number of records
        rec_num = head[2]

        tmp_code = None
        tmp_index = rec_num
       
        count[0] = rec_num 
        while tmp_index:
        
            # read 4 byte after file head
            data = readx2(fp,'I')[0]
        
            # if date is 0xffffffff, it's a record head
            if data[0] == 0xffffffff:
                rec = readx2(fp,fmt_rec_head,position=fp.tell()- 4)[0]
                #tmp_code=rec[1]
                # check if code need to exchange
                tmp_code = rec[1].upper()
                if Maptable.has_key(tmp_code):
                    tmp_code = Maptable[tmp_code] 
                #dict_symbol[tmp_code] = [rec[4]]
                dict_symbol[tmp_code] = []
                #tmp_1day_rec=[]
                #time_tag = None
                tmp_index = tmp_index - 1
                #print tmp_index, tmp_code, rec[4]
            # else, it's a record with time 
            else: 
                # read 48 rec one times if minute dad
                rec = readx2(fp, fmt_rec_data,
                        position=fp.tell()- 4,
                        number=rec_number_block)
                if len(rec) == 0: continue
                
                # remove last unkown element of each record
                map(lambda x:x.pop(), rec)
                # convert to array
                rec_ = np.rec.array(rec, 
                                formats=QUOTE_DAY_ARR_FMT,
                                names=QUOTE_DAY_TITLE) 
                if out_str:
                    rec_ = quote2str(rec,time_fmt=time_num2str_fmt)
                
                # append rec into dic 
                if dict_symbol.has_key(tmp_code):            
                    dict_symbol[tmp_code].append(rec_)
                else:
                    count[1] += 1
                    count[2] = ','.join(count[2],tmp_code)
                    #print '\nLost symbol: %s ' %tmp_code
    
    except:
        raise

    #tlist=[x for x in abnormal_list[1][1:] if x not in abnormal_list[2][1:]]
    err = 'Total {0} stock, {1} failed,  error code: {2}'.format(*count)
    
    return [dict_symbol,err] 

def datedelta(datapath,date_ref= None):
    err ='# They are up to date.'
    lst=[]
    file= 'sh000001.txt' if date_ref is None else date_ref
    try: 
        with open(os.path.join(datapath,file)) as fp:
                data=[i.strip('\n') for i in fp.readlines() if len(
                    i.strip('\n').strip(' '))>0 ]

        startdate=datetime.date(*strptime(data[-1].split(',')[0], 
                                            "%Y%m%d")[0:3])
        
        
        d= startdate
        while 1:
            d += datetime.timedelta(1)
            if d > datetime.date.today(): 
                break
            if d.isoweekday( ) < 6: 
                lst.append(d.strftime("%Y%m%d"))
        
    except:
        #print Error 
        err = str(sys.exc_info()[:2])
        
    return lst, err           
        
def main():
   pass  
   

if __name__ == "__main__":
    main() 


