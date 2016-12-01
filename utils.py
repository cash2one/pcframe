#coding=utf-8
#coding=utf-8
#!/usr/bin/env python

import re
import time

sid_p = re.compile(r"rsv_sid=([\d_]+)")
area_p = re.compile(r"pos=([a-zA-Z]+)")
srcid_p = re.compile(r"rsv_srcid=(\d+)")
zhixin_tq_p = re.compile(r"rsv_zhixin_tq=([^:]+)")
ad_right_old_p = re.compile(r"adRightOld=([\d]+)")
ad_right_new_p = re.compile(r"adRightNew=([\d]+)")
ad_zx_p = re.compile(r"advV2=([\d]+)")
tpl_p = re.compile(r"rsv_tpl=([a-zA-Z]+_[a-zA-Z]+)")

def getTpl(tp):
    if not tp:
        return None
    tpl_m = tpl_p.search(tp)
    if not tpl_m:
        return None
    tpl = tpl_m.group(1)
    return tpl

def splitTp( tp):
    tp_dict = {}
    if tp is not None:
        fields = tp.split( ':')
        for f in fields:
            k_v = f.split( '=')
            if len( k_v) == 2:
                if k_v[ 0] not in tp_dict:
                    tp_dict[ k_v[ 0]] = k_v[ 1]
    return tp_dict

def getSrcidFromDisplay(url_info):
    source = url_info.attr('source')
    srcid = None
    if source == 'SPR' or source == 'SPT':
        srcid = url_info.attr("url_info.resourid")
    else:
        srcid = url_info.attr("url_info.srcid")
    return srcid

def getSrcid(tp, search=None, action=None):
    if not tp and ( not search or not action):
        return None
    srcid = None
    if tp:
        srcid_m = re.search(srcid_p, tp)
        if srcid_m:
            srcid = srcid_m.group(1)

    if action and search and not srcid:
        index = action.attr("index")
        if index == None :
            return None
        url_info=search.attr("urls_info")[index]
        srcid = getSrcidFromDisplay(url_info)

    if srcid:
        srcid = str(srcid)
        return srcid

    return None

def getTQ(tp):
    if not tp:
        return None

    tq_m = zhixin_tq_p.search(tp)
    if not tq_m:
        return None

    tq = tq_m.group(1)
    return tq

def getXpath(tp) :
    rsv_list = tp.split(':')
    xpath = None
    for item in rsv_list:
        if item.startswith('rsv_xpath='):
            xpath_list = item.split('=')
            if len(xpath_list) == 2:
                xpath = xpath_list[1].strip()
    return xpath

def gettime(tm):
    tm_int = int(time.mktime(time.strptime(tm, '%d/%b/%Y:%H:%M:%S')))
    return tm_int

def getdate(tm):
    tm = time.strptime(tm, '%d/%b/%Y:%H:%M:%S')
    mon = tm.tm_mon
    if mon < 10:
        mon = '0' + str(mon)
    day = tm.tm_mday
    if day < 10:
        day = '0' + str(day)
    tmstr = str(tm.tm_year) + str(mon) + str(day)
    return tmstr

def parseDate(datestr,step = 1):
    dates=[]
    days=[0,31,28,31,30,31,30,31,31,30,31,30,31]
    if datestr.find('-')>0:
        date=datestr[:8]
        if not (date.isdigit() and datestr[-8:].isdigit()):
            sys.exit(0)
        dates.append(date)
        cnt = 0
        while (date != datestr[-8:]):
            year=int(date)/10000
            m_day=int(date)%10000
            month=m_day/100
            day=m_day%100
            maxday=days[month]
            if (day==maxday):
                if month==12:
                    year+=1
                    month=1
                else:
                    month+=1
                day=1
            else:
                day+=1
            date=str(year*10000+month*100+day)

            cnt += 1
            cnt = cnt%step
            if cnt == 0:
                dates.append(date)

    else:
        dates.append(datestr)
    return dates

def checkSid(tp, sid):
    sid_m = sid_p.search(tp)
    if not sid_m :
        return False
    sid_str = sid_m.group(1)
    sid_list = sid_str.split("_")
    if sid in sid_list :
        return True
    return False
    
if __name__ == '__main__':

    #dates = parseDate('20130301-20130401',7);
    print getTpl('rsv_tpl=vd_mininewest')

    
