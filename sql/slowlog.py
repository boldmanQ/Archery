# -*- coding: UTF-8 -*-
import re
import json
import pytz
import logging
import datetime
import hashlib

from common.utils.aliyun_sdk import Aliyun

from django.core import serializers
from django.db.models.functions import Trunc
from django.core.serializers.json import DjangoJSONEncoder
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import permission_required
from django.db.models import F, Sum, Value as V, Max, Min, Count, CharField, Avg, IntegerField
from django.db.models.functions import Cast
from django.db.models.functions import Concat
from django.http import HttpResponse
from django.views import View
from django.views.generic import ListView
from sql.utils.resource_group import user_instances
from common.utils.extend_json_encoder import ExtendJSONEncoder
from .models import Instance, AliyunRdsConfig,\
    SlowQuery, SlowQueryDetail

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
            #start_time, end_time = AliyunAPI.get_hour_date_times()
            start_time = request.GET.get('StartTime')
            end_time = request.GET.get('EndTime')
            response = AliyunAPI.DescribeSlowLogRecords(rds_dbinstanceid, start_time, end_time, dbtype, PageNumber=1)
            response = json.loads(response)
            record_count = int(response['TotalRecordCount'])
            page_size = response['PageRecordCount']
            if record_count == 0:
                # 接口返回数据为空
                return HttpResponse({'status': 'success', 'data': None})
            elif record_count <= page_size:
                # 接口返回仅一页数据
                self.SaveSlowQueryLog(instance, response['Items'])
                return HttpResponse(json.dumps(response))
            else:
                page_size = response['PageRecordCount']
                total_page_num = int((record_count - 1) / page_size + 1)
                for page_num in range(2, total_page_num + 1):
                    response = AliyunAPI.DescribeSlowLogRecords(rds_dbinstanceid, start_time, end_time, dbtype, PageNumber=page_num)
                    response = json.loads(response)
                    self.SaveSlowQueryLog(instance, response['Items'])
                    return HttpResponse(json.dumps(response))
        else:
            # 非阿里云实例
            pass

    def sql_analysis(self, sqltext):
        if sqltext.lower().startswith('select'):
            result = re.match('(select)(.*)(from)(.*)(where)(.*)', sqltext, re.DOTALL|re.I)
            if result:
                result = result.groups()
                sample_text = ' '.join(result[0:-2]) + ' {}'.format(result[-1].split(' ')[0])
            else:
                sample_text = sqltext
        elif sqltext.lower().startswith('update'):
            result = re.match('(update)(.*)(set)(.*)(where)(.*)', sqltext, re.DOTALL|re.I)
            if result:
                result = result.groups()
                sample_text = ' '.join(result[0:-2]) + ' {}'.format(result[-1].split(' ')[0])
            else:
                sample_text = sqltext
        else:
            sample_text = sqltext
        checksum = hashlib.md5(sample_text.encode()).hexdigest()
        print(checksum, sample_text)
        return checksum, sample_text


    def SaveSlowQueryLog(self, instance, query_data, *args, **kwargs):

        if instance.db_type == 'mongodb':
            response_data = query_data['LogRecords']
        elif instance.db_type == 'pgsql':
            response_data = query_data['SQLSlowRecord']

        for data in response_data:
            SQLText = data['SQLText']
            checksum, sample_text = self.sql_analysis(SQLText)

            DBName = data['DBName']
            QueryTimes = data['QueryTimes'] if instance.db_type != 'mongodb' else data['QueryTimes']/1000
            ExecutionStartTime = data['ExecutionStartTime']
            HostAddress = data['HostAddress']
            ReturnRowCounts = data['ReturnRowCounts']

            ParseRowCounts = data.get('ParseRowCounts', None)
            KeysExamined = data.get('KeysExamined', None)
            AccountName = data.get('AccountName', None)
            DocsExamined = data.get('DocsExamined', None)

            report_object = SlowQuery.objects.get_or_create(
                checksum=checksum,
                sample=sample_text
            )[0]
            print(report_object,11111)

            SlowQueryDetail.objects.get_or_create(
                DBName=DBName,
                SQLText=SQLText,
                QueryTimes=QueryTimes,
                checksum=report_object,
                HostAddress=HostAddress,
                ReturnRowCounts=ReturnRowCounts,
                InstanceName=instance.instance_name,
                ExecutionStartTime=ExecutionStartTime,

                AccountName=AccountName,
                DocsExamined=DocsExamined,
                KeysExamined=KeysExamined,
                ParseRowCounts=ParseRowCounts,
            )


class SlowQueryDetailView(ListView):
    model = SlowQueryDetail

    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(SlowQueryDetailView, self).dispatch(request, *args, **kwargs)

    def post(self, request):
        start_time = request.POST.get('StartTime')
        end_time = request.POST.get('EndTime')
        db_name = request.POST.get('db_name')
        sql_id = request.POST.get('SQLId')
        instance_name = request.POST.get('instance_name')
        limit = request.POST.get('limit')
        offset = request.POST.get('offset')
        utc = pytz.UTC
        end_time = utc.localize(datetime.datetime.strptime(end_time, '%Y-%m-%d') + datetime.timedelta(days=1))
        start_time = utc.localize(datetime.datetime.strptime(start_time, '%Y-%m-%d'))
        limit = offset + limit
        if sql_id:
            if db_name:
                qs = self.get_queryset().filter(
                    DBName=db_name,
                    checksum=sql_id,
                    InstanceName=instance_name,
                    ExecutionStartTime__range=(start_time, end_time)
                )
            else:
                qs = self.get_queryset().filter(
                    checksum=sql_id,
                    InstanceName=instance_name,
                    ExecutionStartTime__range=(start_time, end_time)
                )
        else:
            if db_name:
                qs = self.get_queryset().filter(
                    InstanceName=instance_name,
                    DBName=db_name,
                    ExecutionStartTime__range=(start_time, end_time)
                )           
            else:
                qs = self.get_queryset().filter(
                    InstanceName=instance_name,
                    ExecutionStartTime__range=(start_time, end_time)
                )   
        slow_sql_record_count = qs.count()
        sql_slow_record = serializers.serialize('json', qs)
        #sql_slow_record = [ data['fields'] for data in sql_slow_record]
        data = json.loads(sql_slow_record)
        result = {"total": slow_sql_record_count, "rows": data}

        # 返回查询结果
        return HttpResponse(json.dumps(result), content_type='application/json')


class SlowQueryReportView(ListView):
    model = SlowQuery

    @method_decorator(csrf_exempt) 
    def dispatch(self, request, *args, **kwargs):
        return super(SlowQueryReportView, self).dispatch(request, *args, **kwargs)

    def post(self, request):
        start_time = request.POST.get('StartTime')
        end_time = request.POST.get('EndTime')
        db_name = request.POST.get('db_name')
        instance_name = request.POST.get('instance_name')
        limit = int(request.POST.get('limit'))
        offset = int(request.POST.get('offset'))
        utc = pytz.UTC
        end_time = utc.localize(datetime.datetime.strptime(end_time, '%Y-%m-%d') + datetime.timedelta(days=1))
        start_time = utc.localize(datetime.datetime.strptime(start_time, '%Y-%m-%d'))
        limit = offset + limit
        if db_name:
            qs = self.get_queryset().filter(
                slowquerydetail__DBName=db_name,
                slowquerydetail__InstanceName=instance_name
            ).annotate(
                SQLId=F('checksum'),
            ).values(
                'SQLId'
            ).annotate(
                DBName=Max('slowquerydetail__DBName', output_field=CharField()), QueryTimesTotal=Sum('slowquerydetail__QueryTimes',output_field=IntegerField()),
                QueryTimesMin=Min('slowquerydetail__QueryTimes', output_field=IntegerField()),
                QueryTimesMax=Max('slowquerydetail__QueryTimes', output_field=IntegerField()),
                QueryTimesCount = Count('slowquerydetail__QueryTimes'),
                QueryTimesAvg=Avg('slowquerydetail__QueryTimes'),
                SQLText=Max('slowquerydetail__SQLText', output_field=CharField()),
                QueryDateTime=Trunc('last_seen', 'second', tzinfo=pytz.timezone('Asia/Shanghai')) 
            )
        else:
            qs = self.get_queryset().filter(
                slowquerydetail__InstanceName=instance_name
            ).annotate(
                SQLId=F('checksum'),
            ).values(
                'SQLId'
            ).annotate(
                DBName=Max('slowquerydetail__DBName', output_field=CharField()), QueryTimesTotal=Sum('slowquerydetail__QueryTimes',output_field=IntegerField()),
                QueryTimesMin=Min('slowquerydetail__QueryTimes', output_field=IntegerField()),
                QueryTimesMax=Max('slowquerydetail__QueryTimes', output_field=IntegerField()),
                QueryTimesCount = Count('slowquerydetail__QueryTimes'),
                QueryTimesAvg=Avg('slowquerydetail__QueryTimes'),
                SQLText=Max('slowquerydetail__SQLText', output_field=CharField()),
                QueryDateTime=Trunc('last_seen', 'second', tzinfo=pytz.timezone('Asia/Shanghai')) 
            )
        slow_sql_record_count = qs.count()
        slow_sql_list = qs.order_by('-QueryTimesTotal', '-QueryTimesAvg')[offset:limit]
        slow_sql_list = list(slow_sql_list.values())
        #data = json.dumps(slow_sql_list)
        result = {"total": slow_sql_record_count, "rows": slow_sql_list}

        # 返回查询结果
        return HttpResponse(json.dumps(result, cls=DjangoJSONEncoder), content_type='application/json')