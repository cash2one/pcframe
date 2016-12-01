# coding=utf8

import sys
if len(sys.argv) > 1:
    module = sys.argv[1]
    sys.path.append(module)
import time
import log_parser
import datetime
import worker
import utils
import filter
import random
import reducer
import os

WORKER = None
KEYS = []
KVS = {}
WHITE_LIST = {}
MAP_DIC = {0: '22', 1: '23', 2: '24', 3: '25', 4: '26', 5: '27', 6: '273', 7: '274', 8: '275', 9: '276', 10: '277', 11: '160', 12: '161', 13: '162', 14: '163', 15: '164', 16: '165', 17: '166', 18: '167', 19: '168', 20: '169', 21: '214', 22: '215', 23: '1000', 24: '1001', 25: '100', 26: '1003', 27: '1004', 28: '1005', 29: '1006', 30: '1007', 31: '1008', 32: '1009', 33: '108', 34: '109', 35: '281', 36: '282', 37: '283', 38: '284', 39: '285', 40: '286', 41: '287', 42: '170', 43: '171', 44: '172', 45: '173', 46: '174', 47: '175', 48: '176', 49: '1', 50: '178', 51: '179', 52: '224', 53: '225', 54: '1010', 55: '1011', 56: '1012', 57: '1013', 58: '1014', 59: '1015', 60: '1016', 61: '1017', 62: '1018', 63: '1019', 64: '118', 65: '119', 66: '291', 67: '292', 68: '293', 69: '294', 70: '295', 71: '296', 72: '297', 73: '180', 74: '181', 75: '182', 76: '183', 77: '184', 78: '185', 79: '186', 80: '187', 81: '188', 82: '189', 83: '234', 84: '235', 85: '1020', 86: '1021', 87: '1022', 88: '1023', 89: '122', 90: '123', 91: '124', 92: '125', 93: '126', 94: '127', 95: '128', 96: '129', 97: '463', 98: '464', 99: '465', 100: '466', 101: '467', 102: '350', 103: '351', 104: '190', 105: '191', 106: '192', 107: '193', 108: '194', 109: '195', 110: '196', 111: '197', 112: '198', 113: '199', 114: '244', 115: '245', 116: '246', 117: '247', 118: '130', 119: '131', 120: '132', 121: '133', 122: '134', 123: '135', 124: '136', 125: '137', 126: '138', 127: '139', 128: '473', 129: '474', 130: '475', 131: '476', 132: '477', 133: '360', 134: '361', 135: '362', 136: '363', 137: '364', 138: '365', 139: '366', 140: '367', 141: '250', 142: '251', 143: '252', 144: '253', 145: '254', 146: '255', 147: '256', 148: '257', 149: '140', 150: '141', 151: '142', 152: '143', 153: '144', 154: '145', 155: '146', 156: '147', 157: '148', 158: '149', 159: '483', 160: '484', 161: '485', 162: '486', 163: '487', 164: '370', 165: '371', 166: '372', 167: '10', 168: '11', 169: '12', 170: '13', 171: '14', 172: '15', 173: '16', 174: '17', 175: '18', 176: '19', 177: '265', 178: '266', 179: '267', 180: '150', 181: '151', 182: '152', 183: '153', 184: '154', 185: '155', 186: '156', 187: '157', 188: '158', 189: '159', 190: '204', 191: '205', 192: '206', 193: '207', 194: '208', 195: '209', 196: '381', 197: '382', 198: '20', 199: '21'}
SID_DIC = {}
SID_HAS_DIC = {}


def init():
    global WORKER
    global KEYS
    global KVS
    global WHITE_LIST
    global SID_DIC
    global SID_HAS_DIC

    path_pre = ''
    if len(sys.argv) >= 2:
        path_pre += module + '/' 
    if filter.CON_LIST:
        if os.path.exists(path_pre + filter.CON_LIST):
            f = open(path_pre + filter.CON_LIST, 'r+')
        elif os.path.exists(filter.CON_LIST):
            f = open(filter.CON_LIST, 'r+')
        while True:
            l = f.readline()
            if not l:
                break
            if '\t' not in l:
                WHITE_LIST[l.strip().lower()] = 1
            else:
                key,value = l.split('\t')
                WHITE_LIST[key.strip().lower()] = int(value.strip())
        f.close()

    KEYS.extend([
        'sid', 'cookie_qid', 'ip', 'pv', 'satisfaction',
        'date', 'time', 'page_stay_time', 'has_page_stay',
        'f', 'tn', 'page_turn', 'query_change',
        'query', 'query_len', 'query_last', 'query_next', 'query_rs', 'rs',
        'first_click_time', 'first_click_type', 'first_click_satisfaction', 'first_click_pos', 'has_first_click',
        'last_click_type', 'last_click_pos', 'last_click_satisfaction',
        'total_click', 'click_pv', 'long_click', 'short_click', 'satisfaction_click',
        'pos1_click', 'pos1_long_click', 'pos1_short_click', 'pos1_satisfaction_click',
        'pos2_click', 'pos2_long_click', 'pos2_short_click', 'pos2_satisfaction_click',
        'pos3_click', 'pos3_long_click', 'pos3_short_click', 'pos3_satisfaction_click',
        'pos4_click', 'pos4_long_click', 'pos4_short_click', 'pos4_satisfaction_click',
        'pos5_click', 'pos5_long_click', 'pos5_short_click', 'pos5_satisfaction_click',
        'pos6_click', 'pos6_long_click', 'pos6_short_click', 'pos6_satisfaction_click',
        'pos7_click', 'pos7_long_click', 'pos7_short_click', 'pos7_satisfaction_click',
        'pos8_click', 'pos8_long_click', 'pos8_short_click', 'pos8_satisfaction_click',
        'pos9_click', 'pos9_long_click', 'pos9_short_click', 'pos9_satisfaction_click',
        'pos10_click', 'pos10_long_click', 'pos10_short_click', 'pos10_satisfaction_click',
        'pos11_click', 'pos11_long_click', 'pos11_short_click', 'pos11_satisfaction_click',
        'pos12_click', 'pos12_long_click', 'pos12_short_click', 'pos12_satisfaction_click',
        'pos13_click', 'pos13_long_click', 'pos13_short_click', 'pos13_satisfaction_click',
        'pos14_click', 'pos14_long_click', 'pos14_short_click', 'pos14_satisfaction_click',
        'pos15_click', 'pos15_long_click', 'pos15_short_click', 'pos15_satisfaction_click',
        'pos16_click', 'pos16_long_click', 'pos16_short_click', 'pos16_satisfaction_click',
        'pos17_click', 'pos17_long_click', 'pos17_short_click', 'pos17_satisfaction_click',
        'pos18_click', 'pos18_long_click', 'pos18_short_click', 'pos18_satisfaction_click',
        'pos19_click', 'pos19_long_click', 'pos19_short_click', 'pos19_satisfaction_click',
        'pos20_click', 'pos20_long_click', 'pos20_short_click', 'pos20_satisfaction_click',
        'as_click', 'al_click', 'alop_click', 'alxr_click', 'pp_click', 'ppim_click',
        'im_click', 'lm_click', 'pl_click', 'plr_click', 'behz_click',
        'tab_click', 'tab_music', 'tab_news', 'tab_zhidao', 'tab_pic',
        'tab_video', 'tab_map', 'tab_wenku', 'tab_more', 'tab_tieba','long_long_click' ])

    KVS.update({
        'sid': '-', 'cookie_qid': '-', 'ip': '-', 'pv': 1, 'satisfaction': 0,
        'date': '-', 'time': '-', 'page_stay_time': 0, 'has_page_stay': 0,
        'f': 'enum', 'tn': '-', 'page_turn': 0, 'query_change': 0,
        'query': '-', 'query_len': 0, 'query_last': '-', 'query_next': '-', 'query_rs': '-', 'rs': 0,
        'first_click_time': 0, 'first_click_type': 'enum', 'first_click_satisfaction': 0, 'first_click_pos': 'enum',
        'has_first_click': 0, 'last_click_type': 'enum', 'last_click_satisfaction': 0, 'last_click_pos': 'enum',
        'total_click': 0, 'click_pv': 0, 'long_click': 0, 'short_click': 0, 'satisfaction_click': 0,
        'pos1_click': 0, 'pos1_long_click': 0, 'pos1_short_click': 0, 'pos1_satisfaction_click': 0,
        'pos2_click': 0, 'pos2_long_click': 0, 'pos2_short_click': 0, 'pos2_satisfaction_click': 0,
        'pos3_click': 0, 'pos3_long_click': 0, 'pos3_short_click': 0, 'pos3_satisfaction_click': 0,
        'pos4_click': 0, 'pos4_long_click': 0, 'pos4_short_click': 0, 'pos4_satisfaction_click': 0,
        'pos5_click': 0, 'pos5_long_click': 0, 'pos5_short_click': 0, 'pos5_satisfaction_click': 0,
        'pos6_click': 0, 'pos6_long_click': 0, 'pos6_short_click': 0, 'pos6_satisfaction_click': 0,
        'pos7_click': 0, 'pos7_long_click': 0, 'pos7_short_click': 0, 'pos7_satisfaction_click': 0,
        'pos8_click': 0, 'pos8_long_click': 0, 'pos8_short_click': 0, 'pos8_satisfaction_click': 0,
        'pos9_click': 0, 'pos9_long_click': 0, 'pos9_short_click': 0, 'pos9_satisfaction_click': 0,
        'pos10_click': 0, 'pos10_long_click': 0, 'pos10_short_click': 0, 'pos10_satisfaction_click': 0,
        'pos11_click': 0, 'pos11_long_click': 0, 'pos11_short_click': 0, 'pos11_satisfaction_click': 0,
        'pos12_click': 0, 'pos12_long_click': 0, 'pos12_short_click': 0, 'pos12_satisfaction_click': 0,
        'pos13_click': 0, 'pos13_long_click': 0, 'pos13_short_click': 0, 'pos13_satisfaction_click': 0,
        'pos14_click': 0, 'pos14_long_click': 0, 'pos14_short_click': 0, 'pos14_satisfaction_click': 0,
        'pos15_click': 0, 'pos15_long_click': 0, 'pos15_short_click': 0, 'pos15_satisfaction_click': 0,
        'pos16_click': 0, 'pos16_long_click': 0, 'pos16_short_click': 0, 'pos16_satisfaction_click': 0,
        'pos17_click': 0, 'pos17_long_click': 0, 'pos17_short_click': 0, 'pos17_satisfaction_click': 0,
        'pos18_click': 0, 'pos18_long_click': 0, 'pos18_short_click': 0, 'pos18_satisfaction_click': 0,
        'pos19_click': 0, 'pos19_long_click': 0, 'pos19_short_click': 0, 'pos19_satisfaction_click': 0,
        'pos20_click': 0, 'pos20_long_click': 0, 'pos20_short_click': 0, 'pos20_satisfaction_click': 0,
        'as_click': 0, 'al_click': 0, 'alop_click': 0, 'alxr_click': 0, 'pp_click': 0, 'ppim_click': 0,
        'im_click': 0, 'lm_click': 0, 'pl_click': 0, 'plr_click': 0, 'behz_click': 0,
        'tab_click': 0, 'tab_music': 0, 'tab_news': 0, 'tab_zhidao': 0, 'tab_pic': 0,
        'tab_video': 0, 'tab_map': 0, 'tab_wenku': 0, 'tab_more': 0, 'tab_tieba': 0,'long_long_click':0,
        })

    for _srcid in filter.SRCID_LIST:
        KEYS.append('src_' + str(_srcid) + '_disp')
        KEYS.append('src_' + str(_srcid) + '_pos')
        KEYS.append('src_' + str(_srcid) + '_pos_na')
        KEYS.append('src_' + str(_srcid) + '_click')
        KEYS.append('src_' + str(_srcid) + '_long_click')
        KEYS.append('src_' + str(_srcid) + '_short_click')
        KEYS.append('src_' + str(_srcid) + '_satisfaction_click')
        KEYS.append('src_' + str(_srcid) + '_behz_click')
        KEYS.append('src_' + str(_srcid) + '_stay')
        KVS.update({'src_' + str(_srcid) + '_disp': 0})
        KVS.update({'src_' + str(_srcid) + '_pos': 'enum'})
        KVS.update({'src_' + str(_srcid) + '_pos_na': 'enum'})
        KVS.update({'src_' + str(_srcid) + '_click': 0})
        KVS.update({'src_' + str(_srcid) + '_long_click': 0})
        KVS.update({'src_' + str(_srcid) + '_short_click': 0})
        KVS.update({'src_' + str(_srcid) + '_satisfaction_click': 0})
        KVS.update({'src_' + str(_srcid) + '_behz_click': 0})
        KVS.update({'src_' + str(_srcid) + '_stay': 0})

    # USER DEFINE
    WORKER = getattr(worker, filter.WORKER)()
    KEYS.extend(WORKER.getColumns())
    KVS.update(WORKER.getDefaults())
    return {'KEYS': KEYS, 'KVS': KVS}


def process(ml):
    if not filter.filter('cookie', ml, WHITE_LIST):
        return True
    cookie = ml.attr('cookie')
    goals = ml.attr('goals')
    all_searches = []
    for _goal in goals:
        all_searches.extend(_goal.attr('searches'))
    all_idx = -1
    for _goal in goals:
        searches = _goal.attr('searches')
        len_searches = len(searches)
        for (_idx, _search) in enumerate(searches):
            all_idx += 1
            if not filter.filter('search', _search, WHITE_LIST):
            #if not filter.filter('search', _search, all_searches):
                continue
            kvs = KVS.copy()
            actions = _search.attr('actions_info')
            real_actions = []
            for _action in actions:
                if _action.attr('fm') not in ['se', 'inlo']:
                    real_actions.append(_action)
            date_time_s = _search.attr('date_time')
            date_time_c = actions[0].attr('date_time')
            if date_time_s:
                date_time = date_time_s
            elif date_time_c:
                date_time = date_time_c
            else:
                continue
            c = datetime.datetime.strptime(date_time, '%d/%b/%Y:%H:%M:%S')
            kvs['date'] = c.strftime('%Y-%m-%d')
            kvs['time'] = c.strftime('%H:%M:%S')
            # 计算首点、尾点相关信息
            if real_actions:
                session_start_time = utils.gettime(date_time_c)

                first_action = real_actions[0]
                first_click_time = utils.gettime(first_action.attr('date_time')) - session_start_time
                if 0 < first_click_time < 120:
                    kvs['first_click_time'] = first_click_time
                    kvs['has_first_click'] = 1
                else:
                    kvs['first_click_time'] = 0
                fm = first_action.attr('fm')
                kvs['first_click_type'] = fm
                if fm in ['as', 'beha', 'behz'] or (fm.startswith('al') and fm != 'alxr'):
                    kvs['first_click_pos'] = first_action.attr('click_pos')
                if first_action.attr('is_satisfied_click') == 1:
                    kvs['first_click_satisfaction'] = 1
                last_action = real_actions[-1]
                page_stay_time = utils.gettime(last_action.attr('date_time')) - session_start_time
                if page_stay_time > 0:
                    kvs['page_stay_time'] = page_stay_time
                    kvs['has_page_stay'] = 1
                fm = last_action.attr('fm')
                kvs['last_click_type'] = fm
                if fm in ['as', 'beha', 'behz'] or (fm.startswith('al') and fm != 'alxr'):
                    kvs['last_click_pos'] = last_action.attr('click_pos')
                if last_action.attr('is_satisfied_click') == 1:
                    kvs['last_click_satisfaction'] = 1
            query_info = _search.attr('query_info')
            query = query_info.attr('query')
            # 计算通用信息
            kvs['query'] = query
            kvs['query_len'] = len(query.decode('gb18030'))
            kvs['f'] = query_info.attr('f')
            kvs['ip'] = _search.attr('ip')
            kvs['cookie_qid'] = cookie + '_' + _search.attr('qid')
            kvs['tn'] = _search.attr('tn')
            kvs['satisfaction'] = _search.attr('satisfaction')
            # 计算前一个query和后一个query
            if _idx > 0:
                kvs['query_last'] = searches[_idx - 1].attr('query_info.query')
            if _idx + 1 < len_searches:
                kvs['query_next'] = searches[_idx + 1].attr('query_info.query')
            # 计算翻页、换query、rs
            _all_idx = all_idx + 1
            # 换query、翻页是同一个goal
            for _se in searches[(_idx + 1):]:
                _all_idx += 1
                page_no = _se.attr('page_no')
                if page_no == 1:
                    query_info = _se.attr('query_info')
                    new_query = query_info.attr('query')
                    f = query_info.attr('f')
                    if f in ['3', '8']:
                        if kvs['query_len'] <= 7:
                            if new_query != query and query in new_query:
                                kvs['query_change'] = 1
                        else:
                            if new_query != query:
                                kvs['query_change'] = 1

                    elif f == '1':
                        kvs['rs'] = 1
                        kvs['query_rs'] = new_query
                    break
                kvs['page_turn'] += 1
            else:
                # rs是跨goal的
                for _se in all_searches[_all_idx:]:
                    page_no = _se.attr('page_no')
                    if page_no == 1:
                        f = _se.attr('query_info.f')
                        if f == '1':
                            kvs['rs'] = 1
                            kvs['query_rs'] = _se.attr('query_info.query')
                        break
            tp_dict = utils.splitTp(actions[0].attr('tp'))
            sids = tp_dict.get('rsv_sid','').split('_')
            for _s in filter.SID_LIST:
                if str(_s) in sids:
                    kvs['sid'] = _s
                    break
            len_real_actions = len(real_actions)
            src_act = {}  # 特定卡片的点击
            # 计算点击相关信息
            action_info_list = []
            for (_nex_act, _action) in enumerate(real_actions, 1):
                fm = _action.attr('fm')
                if fm == 'tab':  # 单独统计tab点击
                    kvs['tab_click'] += 1
                    tab = _action.attr('tab')
                    if tab in ['music', 'news', 'zhidao', 'pic', 'video', 'map', 'wenku', 'more', 'tieba']:
                        kvs['tab_' + tab] += 1
                else:
                    kvs['total_click'] += 1  # 包括交互

                    is_satisfied_click = _action.attr('is_satisfied_click')
                    if is_satisfied_click == 1:
                        kvs['satisfaction_click'] += 1  # 满意点击
                    t1 = utils.gettime(_action.attr('date_time'))
                    _l = ''
                    if fm not in ['beha', 'behz']:  # 长点击不包括交互
                        if _nex_act != len_real_actions:
                            t2 = utils.gettime(real_actions[_nex_act].attr('date_time'))
                            dura = t2 - t1  # 当前点击的时间与后一个用户点击的时间差值
                            real_dura_time = t2 - t1
                        else:  # 如果为最后一次点击
                            si = _idx + 1
                            dura = 0
                            real_dura_time = 0
                            if si < len_searches:  # goal的非最后一个search
                                t2 = searches[si].attr('date_time')
                                if t2:
                                    dura = utils.gettime(t2) - t1
                                    real_dura_time = utils.gettime(t2) - t1
                            elif is_satisfied_click == 1:
                                dura = 40
                                real_dura_time = 120
                        if dura >= 40:
                            _l = 'long_click'
                        elif dura < 5:
                            _l = 'short_click'
                        else:
                            _l = ''
                        if real_dura_time >= 120:
                            kvs['long_long_click'] += 1
                        click_pos = _action.attr('click_pos')
                        action_tuple = (fm, click_pos, _l)
                        action_info_list.append(action_tuple)
                    if _l:
                         kvs[_l] += 1  # 长点击/短点击
                    if filter.SRCID_LIST:  # 统计卡片相关的点击信息
                        tp = _action.attr('tp')
                        tp_dict = utils.splitTp(tp)
                        srcid = tp_dict.get('rsv_srcid', '0')
                        if int(srcid) in filter.SRCID_LIST:
                            kvs['src_' + srcid + '_click'] += 1  # 卡片的点击
                            if fm in ['beha', 'behz']:
                                kvs['src_' + srcid + '_behz_click'] += 1  # 卡片的交互点击
                            if _l:
                                kvs['src_' + srcid + '_' + _l] += 1  # 卡片的长点击/短点击
                            if is_satisfied_click == 1:
                                kvs['src_' + srcid + '_satisfaction_click'] += 1  # 卡片的满意点击
                            if srcid not in src_act:
                                src_act[srcid] = []
                            src_act[srcid].append(_action)  # 记录下卡片的点击
                    if fm in ['pp', 'ppim', 'im', 'lm', 'pl', \
                            'plr', 'alxr', 'alop', 'as']:
                        _k = fm
                    elif fm in ['behz', 'beha']:
                        _k = 'behz'
                    elif fm.startswith('al'):
                        _k = 'al'
                    else:
                        continue
                    kvs[_k + '_click'] += 1  # 不同类型的点击
                    # 分位置点击
                    if _k in ['alop', 'as', 'al', 'behz']:
                        click_pos = _action.attr('click_pos')
                        tp = _action.attr('tp')
                        tp_dict = utils.splitTp(tp)
                        rsv_tpl = tp_dict.get('rsv_tpl', '-')
                        if _k == 'behz' and rsv_tpl.startswith('right_'):
                            continue
                        if 0 < click_pos < 21:
                            kvs['pos' + str(click_pos) + '_click'] += 1
                            if _l:
                                kvs['pos' + str(click_pos) + '_' + _l] += 1
                            if is_satisfied_click == 1:
                                kvs['pos' + str(click_pos) + '_satisfaction_click'] += 1
            if filter.SRCID_LIST:
                urls = _search.attr('urls_info')
                for _url in urls:  # 卡片的展现信息
                    srcid = utils.getSrcidFromDisplay(_url)
                    if srcid and int(srcid) in filter.SRCID_LIST:
                        kvs['src_' + str(srcid) + '_disp'] = 1
                        dis_pos = _url.attr('display_pos')
                        if dis_pos > 0:
                            kvs['src_' + str(srcid) + '_pos'] = dis_pos
                        na_pos = _url.attr('natural_pos')
                        if na_pos > 0:
                            kvs['src_' + str(srcid) + '_pos_na'] = na_pos
                for _src in src_act:  # 卡片的停留时间
                    if src_act[_src][-1].attr('satisfaction_click') == 1:
                        _d = 200
                    elif src_act[_src] == real_actions[-1] and \
                            src_act[_src][-1].attr('fm') == 'alop':
                        _d = 20
                    else:
                        f = utils.gettime(src_act[_src][0].attr('date_time'))
                        l = utils.gettime(src_act[_src][-1].attr('date_time'))
                        _d = l - f
                        if _d > 200:
                            _d = 200
                    kvs['src_' + _src + '_stay'] = _d
            if kvs['total_click'] > 0:
                kvs['click_pv'] = 1
            kvs.update(WORKER.getValues(_search, WHITE_LIST,kvs,action_info_list))
            # 输出PV级别数据 
            print (MAP_DIC[random.randint(50, 99)] + '\t' + '\t'.join([str(kvs[x]) for x in KEYS])).decode('gb18030').encode('utf8')
            # 输出query级别数据
            print (MAP_DIC[hash(kvs['query']) % 48 + 2] + '\t' + kvs['query'] + '\t' + '\t'.join([str(kvs[x]) for x in KEYS])).decode('gb18030').encode('utf8')
            
            # 这里是干什么？
            tmp_date = kvs['date']
            if filter.URL_FLAG == True:
                for i in range(0, 10):
                    if WHITE_LIST.get(_search.attr('urls_list')[i].attr('url')):
                        tmp_sid = str(kvs['sid']) + '@' + WHITE_LIST[_search.attr('urls_list')[i].attr('url')]
                    else:
                        tmp_sid = str(kvs['sid'])
            else:
                tmp_sid = str(kvs['sid'])
                
            # 构建SID_DIC和SID_HAS_DIC
            if not SID_DIC.get(tmp_sid):
                SID_DIC[tmp_sid] = {}
                SID_DIC[tmp_sid][tmp_date] = {}
            else:
                if not SID_DIC[tmp_sid].get(tmp_date):
                    SID_DIC[tmp_sid][tmp_date] = {}
                    
            if not SID_HAS_DIC.get(tmp_sid):
                SID_HAS_DIC[tmp_sid] = {}
                SID_HAS_DIC[tmp_sid][tmp_date] = {}
            else:
                if not SID_HAS_DIC[tmp_sid].get(tmp_date):
                    SID_HAS_DIC[tmp_sid][tmp_date] = {}
                    
            
            for idx, name in enumerate(KEYS):
                if not KVS[name] == '-':
                    if KVS[name] == 'enum':
                        if not SID_DIC[tmp_sid][tmp_date].get(name):
                            SID_DIC[tmp_sid][tmp_date][name] = {}
                        if SID_DIC[tmp_sid][tmp_date][name].get(kvs[name]):
                            SID_DIC[tmp_sid][tmp_date][name][kvs[name]] += 1
                        else:
                            SID_DIC[tmp_sid][tmp_date][name][kvs[name]] = 1
                    else:
                        if SID_DIC[tmp_sid][tmp_date].get(name):
                            SID_DIC[tmp_sid][tmp_date][name] += kvs[name]
                        else:
                            SID_DIC[tmp_sid][tmp_date][name] = kvs[name]
                        if SID_HAS_DIC[tmp_sid][tmp_date].get(name):
                            if kvs[name] > 0:
                                SID_HAS_DIC[tmp_sid][tmp_date][name] += 1
                        else:
                            if kvs[name] > 0:
                                SID_HAS_DIC[tmp_sid][tmp_date][name] = 1
                            else:
                                SID_HAS_DIC[tmp_sid][tmp_date][name] = 0


def dic_has_attr(_dic, keys, kvs):
    tmp_list = []
    for k in keys:
        if kvs[k] == 'enum' or kvs[k] == '-':
            tmp_list.append('null')
        else:
            tmp_list.append(str(_dic[k]))
    return tmp_list

def dic_attr(_dic, keys, kvs):
    tmp_list = []
    for k in keys:
        if kvs[k] == 'enum':
            tmp_str = ''
            for v in _dic[k]:
                tmp_str = tmp_str + str(v) + ':' + str(_dic[k][v]) + '_'
            tmp_list.append(tmp_str[:-1])
        elif kvs[k] == '-':
            tmp_list.append('null')
        else:
            tmp_list.append(str(_dic[k]))
    return tmp_list

if __name__ == '__main__':
    init()
    ml = log_parser.MergeLog_Protobuf()
    while True:
        try:
            flag = ml.readNext()
        except Exception, e:
            continue
        if flag <= 0:
            break
        process(ml)
    for tmp_sid in SID_DIC:
        for tmp_date in SID_DIC[tmp_sid]:
            print (MAP_DIC[0] + '\t' + str(tmp_sid) + '\t' + str(tmp_date) + '\tadddic\t' + '\t'.join(dic_attr(SID_DIC[tmp_sid][tmp_date], KEYS, KVS))).decode('gb18030').encode('utf8')
            print (MAP_DIC[0] + '\t' + str(tmp_sid) + '\t' + str(tmp_date) + '\thasdic\t' + '\t'.join(dic_has_attr(SID_HAS_DIC[tmp_sid][tmp_date], KEYS, KVS))).decode('gb18030').encode('utf8')
            print (MAP_DIC[1] + '\t' + str(tmp_sid) + '\t' + str(tmp_date) + '\tadddic\t' + '\t'.join(dic_attr(SID_DIC[tmp_sid][tmp_date], KEYS, KVS))).decode('gb18030').encode('utf8')
            print (MAP_DIC[1] + '\t' + str(tmp_sid) + '\t' + str(tmp_date) + '\thasdic\t' + '\t'.join(dic_has_attr(SID_HAS_DIC[tmp_sid][tmp_date], KEYS, KVS))).decode('gb18030').encode('utf8')
