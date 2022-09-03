#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import numpy as np
import pandas as pd
import pymysql as sql
from fuzzywuzzy import fuzz
import configparser as cnf


class ConfigConnector(object):

    def __init__(self):
        self.path_to_me = os.path.abspath(os.curdir) + r'\config.ini'
        self._connect = cnf.ConfigParser()
        if os.path.exists(self.path_to_me):
            self._connect.read(self.path_to_me)
            print('----------------------Найден конфиг----------------------\n')
            self.read_settings()

    def read_settings(self):
        res = {}
        for page in self._connect.sections():
            res[page] = {}
            for head in self._connect.options(page):
                res[page][head] = self._connect.get(page, head)
        if res:
            print('----------------------Конфиг прочитан!----------------------\n')
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
        self.headers = pd.DataFrame()
        if not env:
            self.db_host = cf.get_settings('mysql', 'host')
            self.db_port = cf.get_settings('mysql', 'port')
            self.db_user = cf.get_settings('mysql', 'user')
            self.db_pwd = cf.get_settings('mysql', 'password')
            self.db_name = cf.get_settings('mysql', 'db_name')
            print('----------------------Ключи конфига распознаны!----------------------\n')
            self._connect = sql.connect(host=self.db_host, port=int(self.db_port), user=self.db_user,
                                        password=self.db_pwd, charset='utf8', db=self.db_name,
                                        init_command='set names utf8')
            print('----------------------Подключение с сервером открыто!----------------------\n')

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


class StoregeData():
    storege1 = 0
    storege2 = 0
    df_start = 0
    df_from_calc = 0
    level1 = []
    level2 = []
    level3 = []
    level4 = []
    level5 = []
    filter_chain = []


    def save_data(self, df_from, df_to):
        if df_from.shape[0] == 0 or df_to.shape[0] == 0:
            print('----------------------Cтоп! Данные в хранилище пустые----------------------\n')
            sys.exit()
        if StoregeData.storege1 + StoregeData.storege1 == 0:
            pass
        StoregeData.storege1 = df_from.copy()
        StoregeData.storege2 = df_to.copy()
        print('----------------------Данные сохранены в хранилище!----------------------\n')


def get_ratio(str1, str2):
    print('----------------------Выполнено сравнение!----------------------\n')
    if not str1 or not str2:
        return -1
    return fuzz.token_sort_ratio(str(str1), str(str2))


def var_control(var, type):
    if type.__class__.__name__ == 'int':
        try:
            int(var)
            return True
        except ValueError:
            return False
    return isinstance(var, type)


def start_procedure():
    goods_id = ['GrID_from', 'GrID_to', 'TotalRatio']
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
    columns_compare = columns_compare1 + columns_compare2
    connect = MySqlConnector()
    sql = r'''
        SELECT *
        FROM dim_products
        WHERE CategoryGoods = "Обувь"
        AND TradeMark = "Baldinini"
        AND SeasonCode LIKE "%3"
        OR SeasonCode LIKE "%2"
        LIMIT 10000    
    '''
    param = ''
    if len(param) > 0:
        df = pd.DataFrame(connect.query_all(sql, param))
    else:
        df = pd.DataFrame(connect.query_all(sql))
    print('----------------------Данные из бызы получены!----------------------\n')
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
    print('----------------------Списки значений фильтров подготовлены!----------------------\n')

    total_columns = columns_filter + goods_id + columns_compare1 + columns_compare2
    df_result = pd.DataFrame()
    store = StoregeData()
    res_row = ''
    next_lvl = 0
    count1 = 0

    store.save_data(df, df)
    for s_cod in season_codes['SeasonCode'].iteritems():
        store.level1.append(s_cod)
        df_goods_from = df[df['SeasonCode'] == str(s_cod[1])]
        df_goods_to = df[df['SeasonCode'] != str(s_cod[1])]
        store.save_data(df_goods_from, df_goods_to)
        if df_goods_to.shape[0] == 0 or df_goods_from.shape[0] == 0:
            # df = StoregeData.storege1
            print('----------------------Данные востановлены из хранилища!----------------------\n')
            continue
        print('----------------------Применен фильтр сезона----------------------\n')
        store.save_data(df_goods_from, df_goods_to)
        for tm in trade_mark['TradeMark'].iteritems():
            store.level2.append(tm)
            df_goods_from = df_goods_from[df_goods_from['TradeMark'] == str(tm[1])]
            df_goods_to = df_goods_to[df_goods_to['TradeMark'] == str(tm[1])]
            if df_goods_to.shape[0] == 0 or df_goods_from.shape[0] == 0:
                df_goods_from = StoregeData.storege1
                df_goods_to = StoregeData.storege2
                print('----------------------Данные востановлены из хранилища!----------------------\n')
                continue
            print('----------------------Применен фильтр торговой марки----------------------\n')
            store.save_data(df_goods_from, df_goods_to)
            for sex in sex_goods['Sex'].iteritems():
                store.level3.append(sex)
                df_goods_from = df_goods_from[df_goods_from['Sex'] == str(sex[1])]
                df_goods_to = df_goods_to[df_goods_to['Sex'] == str(sex[1])]
                if df_goods_to.shape[0] == 0 or df_goods_from.shape[0] == 0:
                    df_goods_from = StoregeData.storege1
                    df_goods_to = StoregeData.storege2
                    print('----------------------Данные востановлены из хранилища!----------------------\n')
                    continue
                print('----------------------Применен фильтр Пол---------------------\n')
                store.save_data(df_goods_from, df_goods_to)
                for ctg in category_goods['CategoryGoods'].iteritems():
                    store.level4.append(ctg)
                    df_goods_from = df_goods_from[df_goods_from['CategoryGoods'] == str(ctg[1])]
                    df_goods_to = df_goods_to[df_goods_to['CategoryGoods'] == str(ctg[1])]
                    if df_goods_to.shape[0] == 0 or df_goods_from.shape[0] == 0:
                        df_goods_from = StoregeData.storege1
                        df_goods_to = StoregeData.storege2
                        print('----------------------Данные востановлены из хранилища!----------------------\n')
                        continue
                    print('----------------------Применен фильтр категории----------------------\n')
                    store.save_data(df_goods_from, df_goods_to)
                    for lig in lining_goods['Lining'].iteritems():
                        store.level5.append(lig)
                        df_goods_from = df_goods_from[df_goods_from['Lining'] == str(lig[1])]
                        df_goods_to = df_goods_to[df_goods_to['Lining'] == str(lig[1])]
                        if df_goods_to.shape[0] == 0 or df_goods_from.shape[0] == 0:
                            df_goods_from = StoregeData.storege1
                            df_goods_to = StoregeData.storege2
                            print('----------------------Данные востановлены из хранилища!----------------------\n')
                            continue
                        print('----------------------Применен фильтр подложка----------------------\n')
                        store.save_data(df_goods_from, df_goods_to)
                        next_lvl += 1
                        for _, row_from in df_goods_from.iterrows():
                            for __, row_to in df_goods_to.iterrows():
                                if next_lvl > len(store.filter_chain):
                                    store.filter_chain.append([s_cod, tm, sex, ctg, lig])
                                print('начинаем сверять')
                                if len(res_row) == 0:
                                    res_row = {key: '-1' for key in total_columns}
                                else:
                                    res_row['TotalRatio'] = sum(int(val) if var_control(val, int) and val >= 0 else 0 for val in res_row.values())
                                    df_result = df_result.append(res_row, ignore_index=True)
                                    res_row = {key: -1 for key in total_columns}
                                res_row['GrID_from'] = row_from['GrID']
                                res_row['GrID_to'] = row_to['GrID']
                                for h in columns_filter:
                                    res_row[h] = row_from[h]
                                count1 += 1
                                res = {h: get_ratio(row_from[h], row_to[h]) for h in columns_compare}
                                for k, v in res.items():
                                    res_row[k] = v


    print('Конец! ', f'Выполнено сравнений строк-товаров: {count1}')


if __name__ == "__main__":
    start_procedure()
