#!/usr/bin/env python
# -*- coding:  utf-8 -*-
""" common tools  

"""
# ============================================================================
# imports
# ============================================================================
import datetime as dt

__all__= ["LogDict","dtnum2str","dtstr2num", "is_workingday",
          "get_workingdays", "market_time_series"]

# ============================================================================
# LogDict
# refs to UserDict in python
# ============================================================================

class LogDict:
    """ a dict for log 

    """
    def __init__(self,dict=None, **Kwargs):
        self.data = {}
        if dict is not None:
            self.update(dict)
        if len(kwargs):
            self.update(kwargs)

    def __getitem__(self, key):
        if key in self.data:
            return self.data[key]
        if hasattr(self.__class__, "__missing__"):
            return self.__class__.__missing__(self, key)
        raise KeyError(key)

    def __setitem__(self, key, item):
        if key in self.data:
            self.data[key].append(item)
        else:
            self.data[key] = [item] 
    
    def __str__(self):
        #TODO
        pass

    def keys(self): return self.data.keys()
    def items(self): return self.data.items()
    def iteritems(self): return self.data.iteritems()
    def iterkeys(self): return self.data.iterkeys()
    def itervalues(self): return self.data.itervalues()
    def values(self): return self.data.values()



# ============================================================================
# functions
# ============================================================================
def dtnum2str (num,fmt='%Y%m%d'):
    """ convert datetime int to datetime str
    @num is an int of second since 1970-01-01-0-0-0
    @fmt is str_fmt
    """
    d = dt.datetime(1970,1,1,0,0) + dt.timedelta(seconds=int(num))

    return d.strftime(fmt)

def dtstr2num (dtstr,fmt='%Y%m%d'):
    """ convert datetime str to number since 1970-01-01-0-0-0
    @dtstr: date str, like '20080909' 
    @fmt : str_fmt
    return  an int of second since 1970-01-01-0-0-0
    """
    delta = dt.datetime.strptime(dtstr,fmt) - dt.datetime(1970,1,1,0,0)
    return delta.days*24*60*60+delta.seconds

def market_time_series(step, day=None, 
            in_fmt='%Y%m%d', out_fmt='%Y%m%d,%H%M',
            time_slot=[('0930',120),('1300',120)]):
    """create time series of market open time, 
    
    Parameters
    ----------
    @step: time step in minute, shou be [1,5,10,15,30,60]     
    @day: date base, can be datetime obj or str with format gived in_fmt          
    @in_fmt: string formt for parsing input day if it given as string,
                like day as '20101207', in_fmt as '%Y%m%d'
    @out_fmt: string formt for output time string if given,  
                otherwise return datetime obj
    @time_slot: market open time slots, [(start,dur),(start,dur)]              
        where, start,like 0930, dur -> duration in minutes
    
    Returns
    -------
    result: string or datetime obj list of market  timeserise 
    """
    result=[]
    if day is None:
        obj_day = dt.datetime.today()
    elif isinstance(day, dt.datetime):
        obj_day = day
    elif isinstance(day,str):
        try:
            obj_day=dt.datetime.strptime(day,in_fmt)
        except:
            raise

    str_day = obj_day.strftime('%Y%m%d')        
    
    try:
        for stp, dur in time_slot:
            start = dt.datetime.strptime(str_day+stp,'%Y%m%d%H%M')    
            for x in range(int(dur)/int(step)):
                t = start + dt.timedelta(minutes=int(step))*(x+1) 
                if out_fmt is not None:
                    result.append(t.strftime(out_fmt))
                else:
                    result.append(t)
    except :
        raise
    return result


def load_holiday(fn,fmt='%Y%m%d',sep='-',out_str=False):
    """load holiday data file
    @fn: holiday date file
    @fmt: fmt used to convert str to dat obj
    @sep: seperate used in holiday date file
    @out_str: default is False to output a set of date obj, 
            otherwise date str

    return a frozenset of datetime obj of holiday list
    """
    str2date =lambda s_: dt.datetime.strptime(s_,fmt)
    date2str =lambda d_: dt.datetime.strftime(d_,fmt)

    result = []
    with open(fn,'r') as fp:
        for x in fp.readlines():
            x = x.strip('').rstrip('\n')
            if x.startswith('#') or  len(x) < 1:
                continue
            start, end = map(str2date, x.split(sep))
            lst = [start+dt.timedelta(i) for i in range(
                                            (end-start).days+1)]
            if out_str:
                lst = map(date2str,lst)
            result.extend(lst)
        
    return frozenset(result)


HOLIDAYS = load_holiday('STE_holiday.txt')

def is_workingday(d, fmt='%Y%m%d'):
    """ check given date is a working day
    @d: can be date obj or str with given fmt
    @fmt: str fmt used to parse date

    return True or False
    """

    if isinstance(d,str):
        tmp_d = dt.datetime.strptime(d,fmt)
    else:
        tmp_d = d

    if tmp_d.weekday() in [5,6] or tmp_d in HOLIDAYS:
        return False
    else:
        return True

def get_workingdays(start,end=None, fmt='%Y%m%d', out_str=True):
    """ get workingdays between two dates
    @start: start date, can be str or datetime obj
    @end: end date. if None, end = start + 1 year

    return a generator of working date str or datetime obj
    """

    if isinstance(start,str):
        start = dt.datetime.strptime(start,fmt)

    if end and isinstance(end,str):
        end = dt.datetime.strptime(end,fmt)
    elif end is None:
        end = dt.date(year=start.year + 1,
                      month=start.month,
                      day=start.day)

    tmp_day = start
    while tmp_day < end:
        if is_workingday(tmp_day): 
            if out_str:
                yield tmp_day.strftime(fmt)
            else:
                yield tmp_day
        
        tmp_day += dt.timedelta(1)
     



