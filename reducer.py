# encoding=utf8
"""
Created on 2014年11月24日

@author: Jinyingming
"""

import sys
import mapper
import worker

MAP_DIC = {'214': 21, '215': 22, '265': 177, '133': 121, '132': 120, '131': 119, '130': 118, '137': 125, '136': 124, '135': 123, '134': 122, '139': 127, '138': 126, '225': 53, '24': 2, '25': 3, '26': 4, '27': 5, '20': 198, '21': 199, '22': 0, '23': 1, '281': 35, '283': 37, '282': 36, '285': 39, '284': 38, '287': 41, '286': 40, '1014': 58, '122': 89, '123': 90, '124': 91, '125': 92, '126': 93, '127': 94, '128': 95, '129': 96, '1016': 60, '1020': 85, '1017': 61, '17': 174, '1010': 54, '1022': 87, '1011': 55, '1012': 56, '18': 175, '296': 71, '297': 72, '294': 69, '361': 134, '292': 67, '293': 68, '291': 66, '199': 113, '198': 112, '195': 109, '194': 108, '197': 111, '196': 110, '191': 105, '190': 104, '193': 107, '192': 106, '273': 6, '274': 7, '275': 8, '276': 9, '277': 10, '205': 191, '119': 65, '118': 64, '204': 190, '174': 46, '207': 193, '173': 45, '206': 192, '172': 44, '1018': 62, '1019': 63, '366': 139, '1015': 59, '364': 137, '365': 138, '362': 135, '363': 136, '360': 133, '1013': 57, '474': 129, '245': 115, '381': 196, '108': 33, '109': 34, '370': 164, '367': 140, '100': 25, '295': 70, '244': 114, '224': 52, '382': 197, '246': 116, '1009': 32, '1008': 31, '1007': 30, '1006': 29, '1005': 28, '1004': 27, '1003': 26, '1001': 24, '1000': 23, '176': 48, '152': 182, '179': 51, '178': 50, '252': 143, '253': 144, '250': 141, '251': 142, '256': 147, '257': 148, '254': 145, '255': 146, '371': 165, '175': 47, '182': 75, '183': 76, '180': 73, '181': 74, '186': 79, '187': 80, '184': 77, '185': 78, '1021': 86, '188': 81, '189': 82, '465': 99, '464': 98, '467': 101, '466': 100, '463': 97, '168': 19, '169': 20, '164': 15, '165': 16, '166': 17, '167': 18, '160': 11, '161': 12, '162': 13, '163': 14, '11': 168, '10': 167, '13': 170, '12': 169, '15': 172, '14': 171, '1023': 88, '16': 173, '19': 176, '247': 117, '151': 181, '150': 180, '153': 183, '372': 166, '155': 185, '154': 184, '157': 187, '156': 186, '159': 189, '158': 188, '234': 83, '235': 84, '171': 43, '1': 49, '146': 155, '147': 156, '144': 153, '145': 154, '142': 151, '143': 152, '140': 149, '141': 150, '209': 195, '208': 194, '475': 130, '148': 157, '149': 158, '487': 163, '486': 162, '485': 161, '484': 160, '483': 159, '170': 42, '473': 128, '476': 131, '477': 132, '351': 103, '350': 102, '267': 179, '266': 178}


class reducer_merge(object):
    def __init__(self):
        self.r_sid_dic = {}
        self.r_sid_has_dic = {}
        self.date_sid_dic = {}
        self.date_sid_pos_dic = {}
        self.date_sid_type_dic = {}
        self.date_sid_srcid_dic = {}

    def split_dic(self, str):
        tp_dict = {}
        if str is not None:
            fields = str.split('_')
            for f in fields:
                k_v = f.split(':')
                if len( k_v)==2:
                    if k_v[0] not in tp_dict:
                        tp_dict[ k_v[0]] = k_v[1]
        return tp_dict

    def sid_dim(self, search_list, KEYS, KVS):
        tmp_date = search_list[2]
        tmp_sid = search_list[1]
        if not self.r_sid_dic.get(tmp_date):
            self.r_sid_dic[tmp_date] = {}
            self.r_sid_dic[tmp_date][tmp_sid] = {}
        else:
            if not self.r_sid_dic[tmp_date].get(tmp_sid):
                self.r_sid_dic[tmp_date][tmp_sid] = {}
        if not self.r_sid_has_dic.get(tmp_date):
            self.r_sid_has_dic[tmp_date] = {}
            self.r_sid_has_dic[tmp_date][tmp_sid] = {}
        else:
            if not self.r_sid_has_dic[tmp_date].get(tmp_sid):
                self.r_sid_has_dic[tmp_date][tmp_sid] = {}
        for idx, name in enumerate(KEYS):
            if not KVS[name] == '-':
                if KVS[name] == 'enum':
                    if search_list[3] == 'adddic':
                        if not self.r_sid_dic[tmp_date][tmp_sid].get(name):
                            self.r_sid_dic[tmp_date][tmp_sid][name] = {}
                        for tmp_dic_k in self.split_dic(search_list[idx+4]):
                            if self.r_sid_dic[tmp_date][tmp_sid][name].get(tmp_dic_k):
                                self.r_sid_dic[tmp_date][tmp_sid][name][tmp_dic_k] += int(self.split_dic(search_list[idx + 4])[tmp_dic_k])
                            else:
                                self.r_sid_dic[tmp_date][tmp_sid][name][tmp_dic_k] = int(self.split_dic(search_list[idx + 4])[tmp_dic_k])
                else:
                    if search_list[3] == 'hasdic':
                        if self.r_sid_has_dic[tmp_date][tmp_sid].get(name):
                            self.r_sid_has_dic[tmp_date][tmp_sid][name] += int(search_list[idx+4])
                        else:
                            self.r_sid_has_dic[tmp_date][tmp_sid][name] = int(search_list[idx+4])
                    elif search_list[3] == 'adddic':
                        if self.r_sid_dic[tmp_date][tmp_sid].get(name):
                            self.r_sid_dic[tmp_date][tmp_sid][name] += int(search_list[idx+4])
                        else:
                            self.r_sid_dic[tmp_date][tmp_sid][name] = int(search_list[idx+4])

    def query_dim(self, search_list, KEYS, KVS):
        tmp_query = search_list[1]
        tmp_sid = search_list[2]
        if not self.r_sid_dic.get(tmp_query):
            self.r_sid_dic[tmp_query] = {}
            self.r_sid_dic[tmp_query][tmp_sid] = {}
        else:
            if not self.r_sid_dic[tmp_query].get(tmp_sid):
                self.r_sid_dic[tmp_query][tmp_sid] = {}
        if not self.r_sid_has_dic.get(tmp_query):
            self.r_sid_has_dic[tmp_query] = {}
            self.r_sid_has_dic[tmp_query][tmp_sid] = {}
        else:
            if not self.r_sid_has_dic[tmp_query].get(tmp_sid):
                self.r_sid_has_dic[tmp_query][tmp_sid] = {}
        for idx,name in enumerate(KEYS):
            if not KVS[name] == '-':
                if KVS[name] == 'enum':
                    if not self.r_sid_dic[tmp_query][tmp_sid].get(name):
                        self.r_sid_dic[tmp_query][tmp_sid][name] = {}
                    for tmp_dic_k in self.split_dic(search_list[idx+2]):
                        if self.r_sid_dic[tmp_query][tmp_sid][name].get(tmp_dic_k):
                            self.r_sid_dic[tmp_query][tmp_sid][name][tmp_dic_k] += 1
                        else:
                            self.r_sid_dic[tmp_query][tmp_sid][name][tmp_dic_k] = 1
                else:
                    if self.r_sid_has_dic[tmp_query][tmp_sid].get(name):
                        if int(search_list[idx+2]) > 0:
                            self.r_sid_has_dic[tmp_query][tmp_sid][name] += 1
                    else:
                        if int(search_list[idx+2]) > 0:
                            self.r_sid_has_dic[tmp_query][tmp_sid][name] = 1
                        else:
                            self.r_sid_has_dic[tmp_query][tmp_sid][name] = 0
                    if self.r_sid_dic[tmp_query][tmp_sid].get(name):
                        self.r_sid_dic[tmp_query][tmp_sid][name] += int(search_list[idx+2])
                    else:
                        self.r_sid_dic[tmp_query][tmp_sid][name] = int(search_list[idx+2])

    # PV static
    def pv(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                    else:
                        tmp_group_arr += 0
                tmp_date_arr += str(tmp_group_arr) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] = self.date_sid_dic[tmp_norm_date]+ '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 总体点击率
    def total_click_rate(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['total_click']
                        tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                    else:
                        tmp_group_arr += 0
                        tmp_group_pv += 0
                if tmp_group_pv == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_pv) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 有点击行为比例
    def clicked_pv_rate(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_has_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_has_dic[tmp_norm_date][tmp_norm_sid]['total_click']
                    else:
                        tmp_group_arr += 0
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                    else:
                        tmp_group_pv += 0
                if tmp_group_pv == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_pv) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 换query比例
    def query_change_rate(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_has_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_has_dic[tmp_norm_date][tmp_norm_sid]['query_change']
                    else:
                        tmp_group_arr += 0
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                    else:
                        tmp_group_pv += 0
                if tmp_group_pv == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_pv) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # rs点击率
    def rs_rate(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_has_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_has_dic[tmp_norm_date][tmp_norm_sid]['rs']
                    else:
                        tmp_group_arr += 0
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                    else:
                        tmp_group_pv += 0
                if tmp_group_pv == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_pv) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 翻页率
    def page_turn_rate(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_has_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_has_dic[tmp_norm_date][tmp_norm_sid]['page_turn']
                    else:
                        tmp_group_arr += 0
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                    else:
                        tmp_group_pv += 0
                if tmp_group_pv == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_pv) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 首点时间，只计算有点击的PV
    def first_click_time(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['first_click_time']
                        tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['has_first_click']
                    else:
                        tmp_group_arr += 0
                        tmp_group_pv += 0
                if tmp_group_pv == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_pv) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 页面停留时间
    def page_stay_time(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['page_stay_time']
                        tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['has_page_stay']
                    else:
                        tmp_group_arr += 0
                        tmp_group_pv += 0
                if tmp_group_pv == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_pv) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 长点击率
    def long_click_percent(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['long_click']
                        tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                    else:
                        tmp_group_arr += 0
                        tmp_group_pv += 0
                if tmp_group_pv == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_pv) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 长点击占比
    def long_click_rate(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_click = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['long_click']
                        #tmp_group_click += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['total_click']
                        tmp_group_click += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['total_click'] - self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['behz_click']
                    else:
                        tmp_group_arr += 0
                        tmp_group_click += 0
                if tmp_group_click == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_click) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_120_长点击率
    def long_long_click_percent(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['long_long_click']
                        tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                    else:
                        tmp_group_arr += 0
                        tmp_group_pv += 0
                if tmp_group_pv == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_pv) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 120_长点击占比
    def long_long_click_rate(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_click = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['long_long_click']
                        #tmp_group_click += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['total_click']
                        tmp_group_click += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['total_click'] - self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['behz_click']
                    else:
                        tmp_group_arr += 0
                        tmp_group_click += 0
                if tmp_group_click == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_click) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]
				
    # 短点击率
    def short_click_percent(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['short_click']
                        tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                    else:
                        tmp_group_arr += 0
                        tmp_group_pv += 0
                if tmp_group_pv == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_pv) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 短点击占比
    def short_click_rate(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_click = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['short_click']
                        tmp_group_click += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['total_click']  - self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['behz_click']
                    else:
                        tmp_group_arr += 0
                        tmp_group_click += 0
                if tmp_group_click == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_click) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 1-10位搜索结果点击率
    def pos_N_click_rate(self, group):
        for i in range(0,20):
            for tmp_norm_date in self.r_sid_dic:
                tmp_date_arr = ''
                for sid_group in group:
                    tmp_group_arr = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                    tmp_group_pv = 0
                    for tmp_norm_sid in sid_group:
                        if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                            tmp_group_arr[i] += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pos'+str(i+1) + '_click']
                            tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                        else:
                            tmp_group_arr[i] += 0
                            tmp_group_pv += 0
                    if tmp_group_pv == 0:
                        tmp_date_arr += 'error\t'
                    else:
                        tmp_date_arr += str((tmp_group_arr[i]+0.0)/tmp_group_pv) + '\t'
                if self.date_sid_pos_dic.get(tmp_norm_date):
                    self.date_sid_pos_dic[tmp_norm_date] = self.date_sid_pos_dic[tmp_norm_date]+ '\t'+tmp_date_arr[:-1]
                else:
                    self.date_sid_pos_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 1-10位长点击率
    def pos_N_long_click_rate(self, group):
        for i in range(0,10):
            for tmp_norm_date in self.r_sid_dic:
                tmp_date_arr = ''
                for sid_group in group:
                    tmp_group_arr = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                    tmp_group_pv = 0
                    for tmp_norm_sid in sid_group:
                        if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                            tmp_group_arr[i] += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pos'+str(i+1) + '_long_click']
                            tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                        else:
                            tmp_group_arr[i] += 0
                            tmp_group_pv += 0
                    if tmp_group_pv == 0:
                        tmp_date_arr += 'error\t'
                    else:
                        tmp_date_arr += str((tmp_group_arr[i]+0.0)/tmp_group_pv) + '\t'
                if self.date_sid_pos_dic.get(tmp_norm_date):
                    self.date_sid_pos_dic[tmp_norm_date] = self.date_sid_pos_dic[tmp_norm_date]+ '\t'+tmp_date_arr[:-1]
                else:
                    self.date_sid_pos_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 1-10位短点击率
    def pos_N_short_click_rate(self, group):
        for i in range(0,10):
            for tmp_norm_date in self.r_sid_dic:
                tmp_date_arr = ''
                for sid_group in group:
                    tmp_group_arr = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                    tmp_group_pv = 0
                    for tmp_norm_sid in sid_group:
                        if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                            tmp_group_arr[i] += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pos'+str(i+1) + '_short_click']
                            tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                        else:
                            tmp_group_arr[i] += 0
                            tmp_group_pv += 0
                    if tmp_group_pv == 0:
                        tmp_date_arr += 'error\t'
                    else:
                        tmp_date_arr += str((tmp_group_arr[i]+0.0)/tmp_group_pv) + '\t'
                if self.date_sid_pos_dic.get(tmp_norm_date):
                    self.date_sid_pos_dic[tmp_norm_date] = self.date_sid_pos_dic[tmp_norm_date]+ '\t'+tmp_date_arr[:-1]
                else:
                    self.date_sid_pos_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 各种类型的点击率
    def type_click_rate(self, group,click_type):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid][click_type+ '_click']
                        tmp_group_pv += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['pv']
                    else:
                        tmp_group_arr += 0
                        tmp_group_pv += 0
                if tmp_group_pv == 0:
                    tmp_date_arr += 'error\t'
                else:
                    tmp_date_arr += str((tmp_group_arr+0.0)/tmp_group_pv) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    # 资源上的点击
    def src_attr(self, group, srcid):
        if not self.date_sid_srcid_dic.get(srcid):
            self.date_sid_srcid_dic[srcid] = {}
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for src_norm in ['src_'+str(srcid) + '_disp', 'src_'+str(srcid) + '_click', 'src_'+str(srcid) + '_long_click', 'src_'+str(srcid) + '_behz_click', 'src_'+str(srcid) + '_stay']:
                for sid_group in group:
                    tmp_group_arr = 0
                    tmp_group_pv = 0
                    for tmp_norm_sid in sid_group:
                        if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                            tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid][src_norm]
                        else:
                            tmp_group_arr += 0
                    tmp_date_arr += str(tmp_group_arr) + '\t'
            src_norm = 'src_'+str(srcid) + '_has_click'
            for sid_group in group:
                tmp_group_arr = 0
                tmp_group_pv = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_has_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_has_dic[tmp_norm_date][tmp_norm_sid]['src_'+str(srcid) + '_click']
                    else:
                        tmp_group_arr += 0
                tmp_date_arr += str(tmp_group_arr) + '\t'
            for i in range(1,11):
                for sid_group in group:
                    tmp_group_arr = 0
                    for tmp_norm_sid in sid_group:
                        if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                            tmp_group_arr += (int(self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['src_'+str(srcid) + '_pos'][str(i)]) if self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['src_'+str(srcid) + '_pos'].get(str(i)) else 0)
                        else:
                            tmp_group_arr += 0
                    tmp_date_arr += str(tmp_group_arr) + '\t'
            if self.date_sid_srcid_dic[srcid].get(tmp_norm_date):
                self.date_sid_srcid_dic[srcid][tmp_norm_date] = self.date_sid_srcid_dic[srcid][tmp_norm_date]+ '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_srcid_dic[srcid][tmp_norm_date] = tmp_date_arr[:-1]

    def url_click(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['url_click']
                    else:
                        tmp_group_arr += 0
                tmp_date_arr += str(tmp_group_arr) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    def url_long_click(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['url_long_click']
                    else:
                        tmp_group_arr += 0
                tmp_date_arr += str(tmp_group_arr) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    def url_last_click(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['url_last_click']
                    else:
                        tmp_group_arr += 0
                tmp_date_arr += str(tmp_group_arr) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    def url_num(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['url_num']
                    else:
                        tmp_group_arr += 0
                tmp_date_arr += str(tmp_group_arr) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    def url_pos(self, group):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid]['url_pos']
                    else:
                        tmp_group_arr += 0
                tmp_date_arr += str(tmp_group_arr) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    def user_define_add(self, group,user_define_attr):
        for tmp_norm_date in self.r_sid_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_dic[tmp_norm_date][tmp_norm_sid][user_define_attr]
                    else:
                        tmp_group_arr += 0
                tmp_date_arr += str(tmp_group_arr) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

    def user_define_has_add(self, group,user_define_attr):
        for tmp_norm_date in self.r_sid_has_dic:
            tmp_date_arr = ''
            for sid_group in group:
                tmp_group_arr = 0
                for tmp_norm_sid in sid_group:
                    if self.r_sid_has_dic[tmp_norm_date].get(tmp_norm_sid):
                        tmp_group_arr += self.r_sid_has_dic[tmp_norm_date][tmp_norm_sid][user_define_attr]
                    else:
                        tmp_group_arr += 0
                tmp_date_arr += str(tmp_group_arr) + '\t'
            if self.date_sid_dic.get(tmp_norm_date):
                self.date_sid_dic[tmp_norm_date] += '\t'+tmp_date_arr[:-1]
            else:
                self.date_sid_dic[tmp_norm_date] = tmp_date_arr[:-1]

if __name__ == '__main__':
    sid_dic = {}
    flag = 2
    query = ''
    reducer_merge_init = reducer_merge()
    mapper.init()
    norms = []
    groups_dic = {}
    for tmp_value in mapper.WORKER.norm:
        tmp_norm = tmp_value.split(':')[0]
        norms.append(tmp_norm)
        groups = []
        for tmp_group in tmp_value.split(':')[1].split('vs'):
            sid_group = tmp_group.split('_')
            groups.append(sid_group)
        groups_dic[tmp_norm] = groups
    for line in sys.stdin:
        search_list = line.strip().split('\t')
        if MAP_DIC[search_list[0]] == 0:
            flag = 0
            reducer_merge_init.sid_dim(search_list,mapper.KEYS,mapper.KVS)
        elif 1 < MAP_DIC[search_list[0]] < 50:
            flag = 1
            tmp_query = search_list[1]
            if query == '':
                reducer_merge_init = reducer_merge()
                query = tmp_query
            if not query==tmp_query:
                for norm in norms:
                    if norm[:17] == 'pos_N_click_rate' or norm[:5] == 'srcid':
                        continue
                    elif norm[:4] == 'type':
                        eval('reducer_merge_init.type_click_rate')(groups_dic[norm], norm.split('_')[1])
                    elif norm[:4] == 'user':
                        eval('reducer_merge_init.user_define_add')(groups_dic[norm], norm.split('_',1)[1])
                    elif norm[:7] == 'userhas':
                        eval('reducer_merge_init.user_define_has_add')(groups_dic[norm], norm.split('_',1)[1])
                    else:
                        eval('reducer_merge_init.'+norm)(groups_dic[norm])
                for date in reducer_merge_init.date_sid_dic:
                    print date + '\t'+reducer_merge_init.date_sid_dic[date],
                for norm in norms:
                    if norm[:17] == 'pos_N_click_rate':
                        eval('reducer_merge_init.'+norm)(groups_dic[norm])
                for date in reducer_merge_init.date_sid_pos_dic:
                    print '\t'+reducer_merge_init.date_sid_pos_dic[date],
                for norm in norms:
                    if norm[:5] == 'srcid':
                        srcid = norm.split(':')[0].split('_')[1]
                        eval('reducer_merge_init.src_attr')(groups_dic[norm], srcid)
                        for date in reducer_merge_init.date_sid_srcid_dic[srcid]:
                            print '\t'+reducer_merge_init.date_sid_srcid_dic[srcid][date],
                print
                reducer_merge_init = reducer_merge()
                query = tmp_query
            reducer_merge_init.query_dim(search_list,mapper.KEYS,mapper.KVS)
        else:
            flag = 2
            print '\t'.join(line.strip().split('\t')[1:])
    if flag==0:
        print ' \t',
        for norm in norms:
            if norm[:17] == 'pos_N_click_rate' or norm[:5] == 'srcid':
                continue
            elif norm[:4] == 'type':
                eval('reducer_merge_init.type_click_rate')(groups_dic[norm], norm.split('_')[1])
            elif norm[:4] == 'user':
                eval('reducer_merge_init.user_define_add')(groups_dic[norm], norm.split('_',1)[1])
            elif norm[:7] == 'userhas':
                eval('reducer_merge_init.user_define_has_add')(groups_dic[norm], norm.split('_',1)[1])
            else:
                eval('reducer_merge_init.'+norm)(groups_dic[norm])
            print norm,
            for i in range(0,len(groups_dic[norm])):
                print '\t',
        print
        for date in reducer_merge_init.date_sid_dic:
            print date + '\t'+reducer_merge_init.date_sid_dic[date]
        print ' \t',
        for norm in norms:
            if norm[:17] == 'pos_N_click_rate':
                eval('reducer_merge_init.'+norm)(groups_dic[norm])
        for i in range(0,20):
            print 'pos_'+str(i+1) + '_click_rate',
            for j in range(0, len(groups_dic['pos_N_click_rate'])):
                print '\t',
        print
        for date in reducer_merge_init.date_sid_pos_dic:
            print date + '\t'+reducer_merge_init.date_sid_pos_dic[date]
        for norm in norms:
            if norm[:5] == 'srcid':
                srcid = norm.split(':')[0].split('_')[1]
                eval('reducer_merge_init.src_attr')(groups_dic[norm], srcid)
                print ' \t',
                for src_norm in ['src_'+str(srcid) + '_disp', 'src_'+str(srcid) + '_click', 'src_'+str(srcid) + '_long_click', 'src_'+str(srcid) + '_behz_click', 'src_'+str(srcid) + '_stay']:
                    print src_norm,
                    for j in range(0,len(groups_dic[norm])):
                        print '\t',
                print 'src_'+str(srcid) + '_has_click\t',
                for i in range(1,11):
                    print 'src_'+str(srcid) + '_pos_'+str(i),
                    for j in range(0,len(groups_dic[norm])):
                        print '\t',
                print
                for date in reducer_merge_init.date_sid_srcid_dic[srcid]:
                    print date + '\t'+reducer_merge_init.date_sid_srcid_dic[srcid][date]
    elif flag==1:
        for norm in norms:
            if norm[:17] == 'pos_N_click_rate' or norm[:5] == 'srcid':
                continue
            elif norm[:4] == 'type':
                eval('reducer_merge_init.type_click_rate')(groups_dic[norm], norm.split('_')[1])
            elif norm[:4] == 'user':
                eval('reducer_merge_init.user_define_add')(groups_dic[norm], norm.split('_',1)[1])
            elif norm[:7] == 'userhas':
                eval('reducer_merge_init.user_define_has_add')(groups_dic[norm], norm.split('_',1)[1])
            else:
                eval('reducer_merge_init.'+norm)(groups_dic[norm])
        for date in reducer_merge_init.date_sid_dic:
            print date + '\t'+reducer_merge_init.date_sid_dic[date],
        for norm in norms:
            if norm[:17] == 'pos_N_click_rate':
                eval('reducer_merge_init.'+norm)(groups_dic[norm])
        for date in reducer_merge_init.date_sid_pos_dic:
            print '\t'+reducer_merge_init.date_sid_pos_dic[date],
        for norm in norms:
            if norm[:5] == 'srcid':
                srcid = norm.split(':')[0].split('_')[1]
                eval('reducer_merge_init.src_attr')(groups_dic[norm], srcid)
                for date in reducer_merge_init.date_sid_srcid_dic[srcid]:
                    print '\t'+reducer_merge_init.date_sid_srcid_dic[srcid][date],
        print
