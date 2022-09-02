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

    def __ini__(self):
        self.path_to_me = 'config.ini'
        self._connect = cnf.ConfigParser()
        if not os.path.exists(self.path_to_me):
            self._connect.read(self.path_to_me)
            print(self)

    def __str__(self):
        print(self.read_settings())

    def read_settings(self):
        for page in self._connect.sections():
            return {head: self._connect.get(page, head) for head in self._connect.options(page)}

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
            self.db_host = cf.get_settings('MySql', 'host')
            self.db_port = cf.get_settings('MySql', 'port')
            self.db_user = cf.get_settings('MySql', 'user')
            self.db_pwd = cf.get_settings('MySql', 'pass')
            self.db_name = cf.get_settings('MySql', sec)
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


if __name__ == "__main__":
    # date_base = MySqlConnector()
    # sql = ''
    # param = ''
    # df = pd.DataFrame(date_base.query_all(sql, param))

    row = ['1', '2', '3', '4']
    sales = {'Proper': ['erfwefw', 'erfwefw', 'asdasdasd', 'asdasdsad'],
            }
    df = pd.DataFrame(data=sales, index=row)

    def get_ratio(row, str):
        name = row['Proper']
        return fuzz.token_sort_ratio(name, str)
    str = 'erfw'

    s = []

    df.insert(1, 'ratio', df.apply(get_ratio, axis=1, str=str))
    df.insert(2, 'str', str)
    print(df[df['ratio'] > 10])
