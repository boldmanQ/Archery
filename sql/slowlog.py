# -*- coding: UTF-8 -*-
import json
import pytz
import logging
import datetime

from common.utils.aliyun_sdk import Aliyun

from django.core import serializers
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import permission_required
from django.db.models import F, Sum, Value as V, Max
from django.db.models.functions import Concat
from django.http import HttpResponse
from django.views import View
from django.views.generic import ListView
from sql.utils.resource_group import user_instances
from common.utils.extend_json_encoder import ExtendJSONEncoder
from .models import Instance, SlowQuery, SlowQueryHistory, AliyunRdsConfig, SlowQueryHistoryMongoDB, SlowQueryHistoryPostgreSQL

from .aliyun_rds import slowquery_review as aliyun_rds_slowquery_review, \
    slowquery_review_history as aliyun_rds_slowquery_review_history

logger = logging.getLogger('default')


# 获取慢日志明细
class CollectSlowQueryLog(View):

    def get(self, request):
        database = request.GET.get('dbname')
        instance = Instance.objects.filter(instance_name=database)[0]
        # 判断是否为阿里云RDS实例
        if hasattr(instance, 'aliyunrdsconfig'):
            rds_dbinstanceid = instance.aliyunrdsconfig.rds_dbinstanceid
            dbtype = instance.db_type
            AliyunAPI = Aliyun()
            start_time, end_time = AliyunAPI.get_hour_date_times()
            response = AliyunAPI.DescribeSlowLogRecords(rds_dbinstanceid, start_time, end_time, dbtype, PageNumber=1)
            response = json.loads(response)
            record_count = int(response['TotalRecordCount'])
            page_size = response['PageRecordCount']
            if record_count == 0:
                # 接口返回数据为空
                return HttpResponse({'status': 'success', 'data': None})
            elif record_count <= page_size:
                # 接口返回仅一页数据
                self.SaveSlowQueryLog(dbtype, response['Items'])
                return HttpResponse(json.dumps(response))
            else:
                page_size = response['PageRecordCount']
                total_page_num = int((record_count - 1) / page_size + 1)
                for page_num in range(2, total_page_num + 1):
                    response = AliyunAPI.DescribeSlowLogRecords(rds_dbinstanceid, start_time, end_time, dbtype, PageNumber=page_num)
                    response = json.loads(response)
                    self.SaveSlowQueryLog(dbtype, response['Items'])
                    return HttpResponse(json.dumps(response))
        else:
            # 非阿里云实例
            pass

    def SaveSlowQueryLog(self, db_type, query_data, *args, **kwargs):

        if db_type == 'mongodb':
            for slow_record in query_data['LogRecords']:
                SlowQueryHistoryMongoDB.objects.get_or_create(
                    DBName=slow_record['DBName'],
                    DocsExamined=slow_record['DocsExamined'],
                    QueryTimes=slow_record['QueryTimes'] / 1000,
                    AccountName=slow_record['AccountName'],
                    ExecutionStartTime=slow_record['ExecutionStartTime'],
                    SQLText=slow_record['SQLText'],
                    HostAddress=slow_record['HostAddress'],
                    KeysExamined=slow_record['KeysExamined'],
                    ReturnRowCounts=slow_record['ReturnRowCounts'],
                )
        elif db_type == 'pgsql':
            for slow_record in query_data['SQLSlowRecord']:
                SlowQueryHistoryPostgreSQL.objects.get_or_create(
                    DBName=slow_record['DBName'],
                    QueryTimes=slow_record['QueryTimes'],
                    ExecutionStartTime=slow_record['ExecutionStartTime'],
                    SQLText=slow_record['SQLText'],
                    HostAddress=slow_record['HostAddress'],
                    ReturnRowCounts=slow_record['ReturnRowCounts'],
                    ParseRowCounts=slow_record['ParseRowCounts']
                )


class SlowLogQueryRecordView(ListView):
    model = SlowQueryHistoryMongoDB

    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(SlowLogQueryRecordView, self).dispatch(request, *args, **kwargs)

    def post(self, request):
        qs = super().get_queryset()
        start_time = request.POST.get('StartTime')
        end_time = request.POST.get('EndTime')
        db_name = request.POST.get('db_name')
        limit = request.POST.get('limit')
        offset = request.POST.get('offset')
        utc = pytz.UTC
        end_time = utc.localize(datetime.datetime.strptime(end_time, '%Y-%m-%d') + datetime.timedelta(days=1))
        start_time = utc.localize(datetime.datetime.strptime(start_time, '%Y-%m-%d'))
        limit = offset + limit
        if db_name:
            qs = self.get_queryset().filter(
                DBName=db_name,
                ExecutionStartTime__range=(start_time, end_time)
            )
        slow_sql_record_count = qs.count()
        sql_slow_record = serializers.serialize('json', qs)
        #sql_slow_record = [ data['fields'] for data in sql_slow_record]
        data = json.loads(sql_slow_record)
        result = {"total": slow_sql_record_count, "rows": data}

        # 返回查询结果
        return HttpResponse(json.dumps(result), content_type='application/json')
