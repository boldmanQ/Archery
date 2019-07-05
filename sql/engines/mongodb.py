# -*- coding: UTF-8 -*-
"""
@author: boldmanQ
@license: Apache Licence
@file: mongodb.py
@time: 2019/07/03
"""
import pymongo
import logging
import traceback

from . import EngineBase
from .models import ResultSet, ReviewResult, ReviewSet


logger = logging.getLogger('default')


class MongodbEngine(EngineBase):

    def __init__(self, instance=None):
        self.conn = None
        self.thread_id = None
        if instance:
            self.instance = instance
            self.instance_name = instance.instance_name
            self.host = instance.host
            self.port = int(instance.port)
            self.user = instance.user
            self.password = instance.raw_password

    def get_connection(self, db_name=None):
        self.conn = pymongo.MongoClient(host=self.host, port=self.port, username=self.user, password=self.password)
        if db_name:
            self.conn = self.conn[db_name]
        return self.conn

    @property
    def name(self):
        """返回engine名称"""
        return 'MongoDB'

    @property
    def info(self):
        """返回引擎简介"""
        return 'MongoDB engine'

    def get_all_databases(self):
        """获取数据库列表, 返回一个ResultSet，rows=list"""
        result_set = ResultSet()
        try:
            conn = self.get_connection()
            result_set.rows = conn.list_database_names()
        except Exception as e:
            logger.error(f"MongoDB获取databases列表报错，错误信息{traceback.format_exc()}")
            result_set.error = str(e)
        return result_set

    def get_all_tables(self, db_name):
        """获取table 列表, 返回一个ResultSet，rows=list"""
        result_set = ResultSet()
        try:
            conn = self.get_connection()
            db = conn[db_name]
            tables = db.list_collection_names()
            result_set.rows = tables
        except Exception as e:
            logger.error(f"MongoDB获取数据库{db_name} 'tables'报错，错误信息{traceback.format_exc()}") 
        return result_set
    '''
    def get_all_columns_by_tb(self, db_name, tb_name):
        """获取所有字段, 返回一个ResultSet，rows=list"""
        return ResultSet()

    def describe_table(self, db_name, tb_name):
        """获取表结构, 返回一个 ResultSet，rows=list"""
        return ResultSet()

    def query_check(self, db_name=None, sql=''):
        """查询语句的检查、注释去除、切分, 返回一个字典 {'bad_query': bool, 'filtered_sql': str}"""

    def filter_sql(self, sql='', limit_num=0):
        """给查询语句增加结果级限制或者改写语句, 返回修改后的语句"""

    def query(self, db_name=None, sql='', limit_num=0, close_conn=True):
        """实际查询 返回一个ResultSet"""

    def query_masking(self, db_name=None, sql='', resultset=None):
        """传入 sql语句, db名, 结果集,
        返回一个脱敏后的结果集"""
        return resultset

    def execute_check(self, db_name=None, sql=''):
        """执行语句的检查 返回一个ReviewSet"""

    def execute(self):
        """执行语句 返回一个ReviewSet"""

    def get_execute_percentage(self):
        """获取执行进度"""

    def get_rollback(self, workflow):
        """获取工单回滚语句"""

    def get_variables(self, variables=None):
        """获取实例参数，返回一个 ResultSet"""
        return ResultSet()

    def set_variable(self, variable_name, variable_value):
        """修改实例参数值，返回一个 ResultSet"""
        return ResultSet()
    '''

#    @property
#    def server_version(self):
#        """返回引擎服务器版本，返回对象为tuple (x,y,z)"""
#        return tuple()
#    def kill_connection(self, thread_id):
#        """终止数据库连接"""

