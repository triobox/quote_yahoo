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
import logging 
from time import strptime

from tools import dtnum2str

import plac

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

TOHLCVS_ARR_FMT = 'I4,f4,f4,f4,f4,f4,f4' 
TOHLCVS_ARR_NAME= 'time,open,high,low,close,vol,sum' #last is unknow

# split record fmt to str w/o date
SPLIT_STR_FMT = ','.join(['{0:.3f}','{1:.3f}',
                              '{2:.3f}','{3:.3f}'])
SPLIT_ARR_FMT = 'I4,f4,f4,f4,f4' 
SPLIT_ARR_NAME= 'time,sd,ss,ssp,cd' 

# finance record fmt to str w/o date
FIN_STR_FMT = ','.join(['{'+'{0}:.3f'.format(x)+'}' for x in range(37)])
FIN_ARR_FMT = 'I4'+',f4'*37 

# the records number of each stock in DAD file 
REC_NUM_DAILY = 1
REC_NUM_5MIN = 48
REC_NUM_1MIN = 240
# ============================================================================
# Logging
# ============================================================================
# init logging
log = logging.getLogger('fxj_parser') 
# define a handler which writes INFO messages or higher to sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# set a format which is simpler for console use
# tell the handler to use this format
console.setFormatter(
        logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s') ) 
# add the handler to the log 
log.addHandler(console)

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


def readx(file,fmt,position=None, number=1):
    """read data from binary file
    
    parameter
    ---------
    @file: file point
    @fmt: format of data struct, used to unpack binary byte
    @position: posintion in file to start to read, default at file head
    @number: the number of data struct to rread in, default is 1

    return
    ------
    a tule of struct data if number is 1, otherwise a list of tuple 
    """        
    if position == None: position = file.tell()
    file.seek(position)
    size = struct.calcsize(fmt)
    x= file.read(size*number)
    try:
        if number > 1 :
            lst=[struct.unpack(fmt,
                    x[i:i+size]) for i in range(0,size*number,size)]
        else:
            lst=struct.unpack(fmt,x)
    except:
        return None
    
    #if isarray: return np.rec.array(lst, names=name)    
    return lst

def quote2str(quote,time_fmt,out_fmt=OHLCVS_STR_FMT):
    """comvert quote list/array to list of str
    
    parameter
    --------
    @quote: list of quotes, like [(time,o,h,l,c,v,s),...] 
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
    fmt_size = len(out_fmt.split(','))
    result = [','.join([dtnum2str(x[0],fmt=time_fmt),
                        out_fmt.format(*x[1:fmt_size+1])]) for x in quote]
    
    return result

def _iter_record(fp,
                rec_size,
                fmt_rec_head,
                fmt_rec_body,
                out_dtfmt,
                out_strfmt,
                out_arrfmt,
                out_arrname):
    """return a generator of records, [code, data list]
    """
    out_arrfmt_size = len(out_arrfmt.split(','))
    curr_code = ''
    curr_data = []
    while 1:
        # read one record to check is head or body
        raw_data = fp.read(rec_size)
        if len(raw_data) < rec_size: 
            # reach end of file
            break
        
        if raw_data[:4] == '\xff\xff\xff\xff':
            # it's a rec head 
            if curr_code:
                if out_dtfmt:
                    # convert to string  
                    curr_data = quote2str(curr_data,
                                    time_fmt=out_dtfmt,
                                    out_fmt=out_strfmt)
                    #convert to array
                else:
                    curr_data = np.rec.array(curr_data, 
                                        formats=out_arrfmt,
                                        names=out_arrname) 
                rst = [curr_code,curr_data]
                #print rst
                yield rst   
            # reset catch
            curr_code = ''
            curr_data = []
             
            # parse new record head
            rec = struct.unpack(fmt_rec_head,raw_data)

            curr_code = rec[1].upper()
            # check if code need to exchange
            curr_code = Maptable.get(curr_code,curr_code)

        else: # it's a record body
            ## parse one new record body 
            rec = struct.unpack(fmt_rec_body,raw_data)
            curr_data.append(rec[:out_arrfmt_size])

# ============================================================================
# functions
# ============================================================================

def parse_pwr(fp,out_dtfmt='%Y%m%d'):
    """ read dividend & split data (.PWR file)
    
    parameter
    ---------
    @fp: file point of pwr file
    @out_dtfmt: datetime fmt of output string. If given, output are string,
        otherwise, if None or '', output are numpy array.

    return
    ------
    a generator of finance data for one stock each time, like,
        [code, ['20050623,0.000,0.000,0.000,0.125',
                 '20051130,0.320,0.000,0.000,0.000', ...],
    data inside can be str or numpy array, depends on out_dtfmt.
    generator is None if err
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
    #rec_length = 20
    
    rec_size = struct.calcsize(fmt_rec_body)
    flg_size = struct.calcsize(fmt_file_head)

    # read file head
    fp.seek(0)
    raw_data = fp.read(flg_size)
    head = struct.unpack(fmt_file_head,raw_data)
    
    if head[0]!=4282632242 and head[1]!=4291593181:
        log.error('not PWR file: {0}'.format(fp.name))
        return None
    
    return _iter_record(fp=fp,
                rec_size=rec_size,
                fmt_rec_head=fmt_rec_head,
                fmt_rec_body=fmt_rec_body,
                out_dtfmt=out_dtfmt,
                out_strfmt=SPLIT_STR_FMT,
                out_arrfmt=SPLIT_ARR_FMT,
                out_arrname=SPLIT_ARR_FMT)
    

def parse_fin(fp,out_dtfmt='%Y%m%d'):
    """ read finance data (.FIN file) 

    parameter
    ---------
    @fp: file point of fin file
    @out_dtfmt: datetime fmt of output string. If given, output are string,
        otherwise, if None or '', output are numpy array.

    return
    ------
    a generator of finance data for one stock each time, like,
        [code, ['20050623,0.000,0.000,0.000,0.125,...']
    data inside can be str or numpy array, depends on out_dtfmt.
    generator is None if err
    """
    
    def _iter_parse(fp,rec_size,
                    fmt_rec_body,
                    out_dtmft,
                    out_strfmt,
                    out_arrfmt):
        while 1:
            # read one record to check is head or body
            raw_data = fp.read(rec_size)
            if len(raw_data) < rec_size: 
                # reach end of file
                break

            rec = struct.unpack(fmt_rec_body,raw_data)
            
            curr_code = rec[0].upper()+rec[2]
            # check if code need to exchange
            curr_code = Maptable.get(curr_code,curr_code)
            if out_dtfmt:
                # convert to string  
                curr_data = quote2str([rec[4:]],
                                     time_fmt=out_dtmft,
                                        out_fmt=out_strfmt)
            else:
                curr_data = np.rec.array(curr_data, 
                                            formats=out_arrfmt) 
            rst = [curr_code,curr_data]
            yield rst

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
    rec_size = struct.calcsize(fmt_rec_body)
    flg_size = struct.calcsize(fmt_file_head)

    # read file head
    fp.seek(0)
    raw_data = fp.read(flg_size)
    head = struct.unpack(fmt_file_head,raw_data)
    
    if head[0]!=574609676:
        log.error('not FIN file: {0}'.format(fp.name))
        return None

    rec_len = head[1] #166
    
    return _iter_parse( fp=fp,
                        rec_size=rec_size,
                        fmt_rec_body=fmt_rec_body,
                        out_dtmft=out_dtfmt,
                        out_strfmt=FIN_STR_FMT, 
                        out_arrfmt=FIN_ARR_FMT)
    #return 

def parse_dad(fp, out_dtfmt='%Y%m%d,%H%M'): 
    """ read stock quote data from fxj format file (.DAD file)
    
    parameter
    ---------
    @fp: file point of dad file
    @out_dtfmt: datetime fmt of output string. If given, output are string,
        otherwise, if None or '', output are numpy array.

    return
    ------
    a generator of data for one stock each time, like, 
    [code, ['date#1, open, high,low,close,volume,sum', 
          'date#2, open, high,low,close,volume,sum', ...]]
    data inside can be str or numpy array, depends on out_dtfmt.
    generator is None if err
    """
    # filetag:4 (33 FC 19 8C) = 872159628; unknown:4; 
    # record num: 4; unknown: 4 
    fmt_file_head='<IIII' 
    #DATA:4, symbol:8, unknown:4, unknown:4, name:8, unknow:4
    fmt_rec_head = '<I8sII8sI' 
    #DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, SUM, unknow
    fmt_rec_body = '<IffffffI' 
    rec_size = struct.calcsize(fmt_rec_head)
    flg_size = struct.calcsize(fmt_file_head)

    # read file head
    fp.seek(0)
    raw_data = fp.read(flg_size)
    head = struct.unpack(fmt_file_head,raw_data)
    
    if head[0]!= 872159628:
        log.error('not DAD file: {0}'.format(fp.name))
        return 
    
    # number of records
    rec_num = head[2]
    
    return _iter_record(fp=fp,
                rec_size=rec_size,
                fmt_rec_head=fmt_rec_head,
                fmt_rec_body=fmt_rec_body,
                out_dtfmt=out_dtfmt,
                out_strfmt=OHLCVS_STR_FMT,
                out_arrfmt=TOHLCVS_ARR_FMT,
                out_arrname=TOHLCVS_ARR_NAME)

def iter_parser(fname,out_dtfmt):
    """an unify entry for various type fxj file
    @fname: file to be parsed
    
    return:
    an generator of data source 
    """

    base, ext = os.path.splitext(fname)
    ext = ext.lower()
    if os.path.exists(fname) and ext in ['.dad','.pwr','.fin']: 
        fp = open(fname,'rb')
        if ext == '.dad':
            result = parse_dad(fp,out_dtfmt=out_dtfmt)
        elif ext == '.pwr':
            result = parse_pwr(fp,out_dtfmt=out_dtfmt)
        elif ext == '.fin':
            result = parse_fin(fp,out_dtfmt=out_dtfmt)
    else:
        log.error('not a valid FXJ file!')
        return None

    return result

# ============================================================================
# main()
# ============================================================================
@plac.annotations(
    fname=("FXJ data file", 'positional', None),
    code=("print data of given code",'option','c',str,None,"CODE"),
    output=("dump data into file",'option','o',str,None,"OUTPUT"),
        )
def main(fname,code,output):
    "parse FXJ data file"
    
    with open(fname,'rb') as fp:
        base, ext = os.path.splitext(fname)
        if ext.lower() == '.dad':
            result = parse_dad(fp)
        elif ext.lower() == '.pwr':
            result = parse_pwr(fp)
        elif ext.lower() == '.fin':
            result = parse_fin(fp)
        else:
            log.error('not a valid FXJ file!')
            return

        if not result:
            return
        
        if code:
            for r in result:
                if r[0].upper() == code.upper():
                    print('\n'.join(r[1]))    
                    result.close()
                    return
            log.error('{0} not exist  in {1}'.format(code,fname))
            return

        if output:
            with open(output,'w') as ofp:
                count = 0
                for r in result:
                    #print r
                    count += 1
                    for e in r[1]:
                        ofp.writelines(','.join([r[0],e])+'\n')
                log.info('output total {0} stocks'.format(count)) 
            return

if __name__ == "__main__":
    plac.call(main) 

