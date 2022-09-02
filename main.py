#!/usr/bin/python3
# -*- coding: utf-8 -*-

import configparser as cnf
import os
import pymysql as sql
import pandas as pd
import numpy as np
import re
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
        self.headers = []
        if not env:
            sec = 'db_{0}'.format(env)
            self.db_host = cf.get_settings('mysql', 'host')
            self.db_port = cf.get_settings('mysql', 'port')
            self.db_user = cf.get_settings('mysql', 'user')
            self.db_pwd = cf.get_settings('mysql', 'password')
            self.db_name = cf.get_settings('mysql', 'db_name')
            self._connect = sql.connect(host=self.db_host, port=int(self.db_port), user=self.db_user,
                                        password=self.db_pwd, charset='utf8', db=self.db_name,
                                        init_command='set names utf8')

    def query_all(self, query, params=None):
        cursor = self._connect.cursor()
        try:
            if params:
                cursor.execute(query, params)
                self.headers = [i[0] for i in cursor.description]
            else:
                cursor.execute(query)
                self.headers = [i[0] for i in cursor.description]
            result = cursor.fetchall()
        finally:
            cursor.close()
        return result


def get_ratio(row, str_compare, col):
    name_to = row[str(col)]
    return fuzz.token_sort_ratio(str(name_to), str(str_compare))


def start_procedure():
    columns_filter = ['SeasonCode', 'Sex', 'Lining', 'TradeMark', 'CategoryGoods']
    columns_compare1 = ['ArticleA', 'GroupA', 'LineA', 'LineStyleA', 'ModelsA',
                        'GroupA', 'GoodsNameA'
                        ]
    columns_compare2 = ['StyleCode', 'ArticleManufacturer', 'Spoilage', 'Top', 'BackclothType',
                        'ZipType', 'HeelType', 'ToeType', 'Height', 'HeelHeight', 'HeelHightMm',
                        'Details', 'Heel', 'CodeCollor', 'CodeCollorInt', 'Line', 'Material', 'Material2',
                        'MaterialCollor', 'Models', 'ManufactureName', 'GoodsName', 'SubLine', 'Sole', 'Sex',
                        'Manufacture', 'LeatherType', 'FabricStructure', 'FabricStructureRus', 'SoleConnection',
                        'Insole', 'Collor', 'Collor2', 'CollorRus', 'CollorRusAdd', 'Article', 'ManufactureTipycal',
                        'BaseModelGrID', 'Style'
                        ]
    goods_id = ['GrID_from', 'GrID_to', 'TotalRatio']
    count_columns = len(columns_compare1) + len(columns_compare2) + len(goods_id) + len(columns_filter)
    columns_compare = columns_compare1 + columns_compare2

    connect = MySqlConnector()
    sql = f'SELECT * FROM dim_products LIMIT 1000'
    param = ''
    if len(param) > 0:
        df = pd.DataFrame(connect.query_all(sql, param))
    else:
        df = pd.DataFrame(connect.query_all(sql))
    df.columns = connect.headers

    season_codes = df[['SeasonCode']]
    season_codes.fillna(value=np.nan, inplace=True)
    season_codes.dropna(inplace=True)
    season_codes = season_codes.drop_duplicates()
    season_codes['SeasonCodeNum'] = season_codes['SeasonCode'].map(lambda x: re.sub(r"\D", "", x))
    season_codes = season_codes.drop_duplicates()

    season_codes.astype({'SeasonCodeNum': int})
    season_codes['SeasonCodeNum'] = pd.to_numeric(season_codes['SeasonCodeNum'])
    season_codes = season_codes.sort_values(by='SeasonCodeNum', ascending=False)
    season_codes = season_codes[['SeasonCode']]

    trade_mark = df[['TradeMark']]
    trade_mark.fillna(value=np.nan, inplace=True)
    trade_mark.dropna(inplace=True)
    trade_mark = trade_mark.drop_duplicates()
    trade_mark.sort_values(by='TradeMark')

    sex_goods = df[['Sex']]
    sex_goods.fillna(value=np.nan, inplace=True)
    sex_goods.dropna(inplace=True)
    sex_goods = sex_goods.drop_duplicates()
    sex_goods.sort_values(by='Sex')

    category_goods = df[['CategoryGoods']]
    category_goods.fillna(value=np.nan, inplace=True)
    category_goods.dropna(inplace=True)
    category_goods = category_goods.drop_duplicates()
    category_goods.sort_values(by='CategoryGoods')

    lining_goods = df[['Lining']]
    lining_goods.fillna(value=np.nan, inplace=True)
    lining_goods.dropna(inplace=True)
    lining_goods = lining_goods.drop_duplicates()
    lining_goods.sort_values(by='Lining')

    total_columns = columns_filter + goods_id + columns_compare1 + columns_compare2
    df_result = pd.DataFrame()

    for s_cod in season_codes['SeasonCode'].iteritems():
        df_goods_from = df[df['SeasonCode'] == str(s_cod[1])]
        df_goods_to = df[df['SeasonCode'] != str(s_cod[1])]
        for tm in trade_mark['TradeMark'].iteritems():
            df_goods_from = df_goods_from[df_goods_from['TradeMark'] == str(tm[1])]
            df_goods_to = df_goods_to[df_goods_to['TradeMark'] == str(tm[1])]
            for sex in sex_goods['Sex'].iteritems():
                df_goods_from = df_goods_from[df_goods_from['Sex'] == str(sex[1])]
                df_goods_to = df_goods_to[df_goods_to['Sex'] == str(tm[1])]
                for ctg in category_goods['CategoryGoods'].iteritems():
                    df_goods_from = df_goods_from[df_goods_from['CategoryGoods'] == str(ctg[1])]
                    df_goods_to = df_goods_to[df_goods_to['CategoryGoods'] == str(ctg[1])]
                    for lig in lining_goods['Lining'].iteritems():
                        df_goods_from = df_goods_from[df_goods_from['Lining'] == str(lig[1])]
                        df_goods_to = df_goods_to[df_goods_to['Lining'] == str(lig[1])]
                        for row_from in df_goods_from.iterrows():
                            for row_to in df_goods_to.iterrows():
                                print('начинаем сверять')
                                if len(res_row) == 0:
                                    res_row = {key: '-1' for key in total_columns}
                                else:
                                    res_row['TotalRatio'] = sum(res_row[:, columns_compare2[0]:columns_compare2[-1]])
                                    df_result = df_result.append(res_row, ignore_index=True)
                                    res_row = {key: '-1' for key in total_columns}
                                res_row['GrID_from'] = row_from['GrID_from']
                                res_row['GrID_to'] = row_from['GrID_to']
                                for col_name in columns_compare:
                                    res_row[col_name] = row_to.apply(get_ratio, axis=1,
                                                                     str=row_from[col_name], col=col_name)


if __name__ == "__main__":
    start_procedure()
