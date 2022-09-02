#!/usr/bin/python3
# -*- coding: utf-8 -*-

import configparser as cnf
import os
import pymysql as sql
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


class ConfigConnector(object):

    def __init__(self):
        self.path_to_me = os.path.abspath(os.curdir) + r'\config.ini'
        self._connect = cnf.ConfigParser()
        if os.path.exists(self.path_to_me):
            self._connect.read(self.path_to_me)
            self.__str__()

    def __str__(self):
        print(self.read_settings())

    def read_settings(self):
        res = {}
        for page in self._connect.sections():
            res[page] = {}
            for head in self._connect.options(page):
                res[page][head] = self._connect.get(page, head)
        return res

    def get_settings(self, page, header):
        return self._connect.get(page, header)

    def add_settings(self, page, headers=None, values=None):
        if headers:
            if len(headers) == len(values):
                for head, val in headers, values:
                    self._connect.set(page, head, val)
            else:
                return None
        else:
            self._connect.add_section(page)
        with open(self.path_to_me, "w") as config_file:
            self._connect.write(config_file)

    def change_setting(self, page, headers, new_values):
        for head, val in headers, new_values:
            self._connect.set(page, head, val)
        with open(self.path_to_me, "w") as config_file:
            self._connect.write(config_file)

    def del_setting(self, page, headers=None):
        if headers:
            self._connect.remove_section(page)
        else:
            for head in headers:
                self._connect.remove_option(page, head)
        with open(self.path_to_me, "w") as config_file:
            self._connect.write(config_file)


class MySqlConnector(object):

    def __init__(self, env=None):
        cf = ConfigConnector()
        if not env:
            sec = 'db_{0}'.format(env)
            self.db_host = cf.get_settings('mysql', 'host')
            self.db_port = cf.get_settings('mysql', 'port')
            self.db_user = cf.get_settings('mysql', 'user')
            self.db_pwd = cf.get_settings('mysql', 'pass')
            self.db_name = cf.get_settings('mysql', sec)
            self._connect = sql.connect(host=self.db_host, port=int(self.db_port), user=self.db_user,
                                        password=self.db_pwd, charset='utf8', db=self.db_name,
                                        init_command='set names utf8')

    def query_all(self, query, params=None):
        cursor = self._connect.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchall()
        finally:
            cursor.close()
        return result


def start_procedure():
    columns_compare = ['Color', 'Material']
    df_result = pd.DataFrame()
    # date_base = MySqlConnector()
    # sql = f'SELECT {columns_compare} FROM dim_product'
    param = ''
    if len(param) > 0:
        df = pd.DataFrame({'GrID': ['123456', '321654', '789654'],
                            'SeasonCode': ['231d', '231', '231'],
                           'Color': ['asdasd', 'asdasd', 'asdsad'],
                           'Material': ['asdsada', 'qweqweqw', 'dasdsada']
                           })
        # df = pd.DataFrame(date_base.query_all(sql, param))
    else:
        df = pd.DataFrame({'GrID': ['123456', '321654', '789654'],
                            'SeasonCode': ['231d', '231', '231d'],
                           'Color': ['asdasd', 'asdasd', 'asdsad'],
                           'Material': ['asdsada', 'qweqweqw', 'dasdasdas']
                           })
        # df = pd.DataFrame(date_base.query_all(sql))
    df_goods_compare = df[df['SeasonCode'] == '231d']
    df_result['GrID', 'SeasonsCode', 'BaseGrID']  = [df_goods_compare['GrID'], df_goods_compare['SeasonCode'], df_goods_compare['GrID']]
    for col in columns_compare:
        df_goods_compare.apply(get_ratio, axis=1, str=col)
        df['str'] = str
        print(df[df['ratio'] > 10])


def get_ratio(row, str):
    name = row['Proper']
    return fuzz.token_sort_ratio(name, str)


if __name__ == "__main__":
    start_procedure()

