# -*- coding: UTF-8 -*-
from django.db import models
from django.contrib.auth.models import AbstractUser
from common.utils.aes_decryptor import Prpcrypt
from django.utils.translation import gettext as _


class Users(AbstractUser):
    """
    用户信息扩展
    """
    display = models.CharField('显示的中文名', max_length=50, default='')
    failed_login_count = models.IntegerField('失败计数', default=0)
    last_login_failed_at = models.DateTimeField('上次失败登录时间', blank=True, null=True)

    def __str__(self):
        if self.display:
            return self.display
        return self.username

    class Meta:
        managed = True
        db_table = 'sql_users'
        verbose_name = u'用户管理'
        verbose_name_plural = u'用户管理'


DB_TYPE_CHOICES = (
    ('mysql', 'MySQL'),
    ('mssql', 'MsSQL'),
    ('redis', 'Redis'),
    ('pgsql', 'PgSQL'),
    ('oracle', 'Oracle'),
    ('inception', 'Inception'),
    ('goinception', 'goInception'),
    ('mongodb', 'MongoDB')
)


class Instance(models.Model):
    """
    各个线上实例配置
    """
    instance_name = models.CharField('实例名称', max_length=50, unique=True)
    type = models.CharField('实例类型', max_length=6, choices=(('master', '主库'), ('slave', '从库')))
    db_type = models.CharField('数据库类型', max_length=20, choices=DB_TYPE_CHOICES)
    host = models.CharField('实例连接', max_length=200)
    port = models.IntegerField('端口', default=0)
    user = models.CharField('用户名', max_length=100, default='', blank=True)
    password = models.CharField('密码', max_length=300, default='', blank=True)
    charset = models.CharField('字符集', max_length=20, default='', blank=True)
    service_name = models.CharField('Oracle service name', max_length=50, null=True, blank=True)
    sid = models.CharField('Oracle sid', max_length=50, null=True, blank=True)
    create_time = models.DateTimeField('创建时间', auto_now_add=True)
    update_time = models.DateTimeField('更新时间', auto_now=True)

    @property
    def raw_password(self):
        """ 返回明文密码 str """
        pc = Prpcrypt()  # 初始化
        return pc.decrypt(self.password)

    def __str__(self):
        return self.instance_name

    class Meta:
        managed = True
        db_table = 'sql_instance'
        verbose_name = u'实例配置'
        verbose_name_plural = u'实例配置'

    def save(self, *args, **kwargs):
        pc = Prpcrypt()  # 初始化
        if self.password:
            if self.id:
                old_password = Instance.objects.get(id=self.id).password
            else:
                old_password = ''
            # 密码有变动才再次加密保存
            self.password = pc.encrypt(self.password) if old_password != self.password else self.password
        super(Instance, self).save(*args, **kwargs)


class ResourceGroup(models.Model):
    """
    资源组
    """
    group_id = models.AutoField('组ID', primary_key=True)
    group_name = models.CharField('组名称', max_length=100, unique=True)
    group_parent_id = models.BigIntegerField('父级id', default=0)
    group_sort = models.IntegerField('排序', default=1)
    group_level = models.IntegerField('层级', default=1)
    ding_webhook = models.CharField('钉钉webhook地址', max_length=255, blank=True)
    is_deleted = models.IntegerField('是否删除', choices=((0, '否'), (1, '是')), default=0)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)
    users = models.ManyToManyField(Users, through='ResourceGroup2User')
    instances = models.ManyToManyField(Instance, through='ResourceGroup2Instance')

    def __str__(self):
        return self.group_name

    class Meta:
        managed = True
        db_table = 'resource_group'
        verbose_name = u'资源组管理'
        verbose_name_plural = u'资源组管理'


class ResourceGroup2User(models.Model):
    """资源组和用户关联表"""
    resource_group = models.ForeignKey(ResourceGroup, on_delete=models.CASCADE)
    user = models.ForeignKey(Users, on_delete=models.CASCADE)
    create_time = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = 'resource_group_user'
        unique_together = ('resource_group', 'user')
        verbose_name = u'资源组关联用户'
        verbose_name_plural = u'资源组关联用户'


class ResourceGroup2Instance(models.Model):
    """资源组和实例关联表"""
    resource_group = models.ForeignKey(ResourceGroup, on_delete=models.CASCADE)
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    create_time = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = 'resource_group_instance'
        unique_together = ('resource_group', 'instance')
        verbose_name = u'资源组关联实例'
        verbose_name_plural = u'资源组关联实例'


class InstanceTag(models.Model):
    """实例标签配置"""
    tag_code = models.CharField('标签代码', max_length=20, unique=True)
    tag_name = models.CharField('标签名称', max_length=20, unique=True)
    active = models.BooleanField('激活状态', default=True)
    create_time = models.DateTimeField('创建时间', auto_now_add=True)
    instances = models.ManyToManyField(Instance, through='InstanceTagRelations',
                                       through_fields=('instance_tag', 'instance'))

    def __str__(self):
        return self.tag_name

    class Meta:
        managed = True
        db_table = 'sql_instance_tag'
        verbose_name = u'实例标签'
        verbose_name_plural = u'实例标签'


class InstanceTagRelations(models.Model):
    """实例标签关系"""
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    instance_tag = models.ForeignKey(InstanceTag, on_delete=models.CASCADE)
    active = models.BooleanField('激活状态', default=True)
    create_time = models.DateTimeField('创建时间', auto_now_add=True)

    class Meta:
        managed = True
        db_table = 'sql_instance_tag_relations'
        unique_together = ('instance', 'instance_tag')
        verbose_name = u'实例标签关系'
        verbose_name_plural = u'实例标签关系'


SQL_WORKFLOW_CHOICES = (
    ('workflow_finish', _('workflow_finish')),
    ('workflow_abort', _('workflow_abort')),
    ('workflow_manreviewing', _('workflow_manreviewing')),
    ('workflow_review_pass', _('workflow_review_pass')),
    ('workflow_timingtask', _('workflow_timingtask')),
    ('workflow_executing', _('workflow_executing')),
    ('workflow_autoreviewwrong', _('workflow_autoreviewwrong')),
    ('workflow_exception', _('workflow_exception')))


class SqlWorkflow(models.Model):
    """
    存放各个SQL上线工单的基础内容
    """
    workflow_name = models.CharField('工单内容', max_length=50)
    group_id = models.IntegerField('组ID')
    group_name = models.CharField('组名称', max_length=100)
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    db_name = models.CharField('数据库', max_length=64)
    syntax_type = models.IntegerField('工单类型 0、未知，1、DDL，2、DML', choices=((0, '其他'), (1, 'DDL'), (2, 'DML')), default=0)
    is_backup = models.BooleanField('是否备份', choices=((False, '否'), (True, '是'),), default=True)
    engineer = models.CharField('发起人', max_length=30)
    engineer_display = models.CharField('发起人中文名', max_length=50, default='')
    status = models.CharField(max_length=50, choices=SQL_WORKFLOW_CHOICES)
    audit_auth_groups = models.CharField('审批权限组列表', max_length=255)
    run_date_start = models.DateTimeField('可执行起始时间', null=True, blank=True)
    run_date_end = models.DateTimeField('可执行结束时间', null=True, blank=True)
    create_time = models.DateTimeField('创建时间', auto_now_add=True)
    finish_time = models.DateTimeField('结束时间', null=True, blank=True)
    is_manual = models.IntegerField('是否原生执行', choices=((0, '否'), (1, '是')), default=0)

    def __str__(self):
        return self.workflow_name

    class Meta:
        managed = True
        db_table = 'sql_workflow'
        verbose_name = u'SQL工单'
        verbose_name_plural = u'SQL工单'


class SqlWorkflowContent(models.Model):
    """
    存放各个SQL上线工单的SQL|审核|执行内容
    可定期归档或清理历史数据，也可通过``alter table sql_workflow_content row_format=compressed; ``来进行压缩
    """
    workflow = models.OneToOneField(SqlWorkflow, on_delete=models.CASCADE)
    sql_content = models.TextField('具体sql内容')
    review_content = models.TextField('自动审核内容的JSON格式')
    execute_result = models.TextField('执行结果的JSON格式', blank=True)

    def __str__(self):
        return self.workflow.workflow_name

    class Meta:
        managed = True
        db_table = 'sql_workflow_content'
        verbose_name = u'SQL工单内容'
        verbose_name_plural = u'SQL工单内容'


workflow_type_choices = ((1, _('sql_query')), (2, _('sql_review')))
workflow_status_choices = ((0, '待审核'), (1, '审核通过'), (2, '审核不通过'), (3, '审核取消'))


class WorkflowAudit(models.Model):
    """
    工作流审核状态表
    """
    audit_id = models.AutoField(primary_key=True)
    group_id = models.IntegerField('组ID')
    group_name = models.CharField('组名称', max_length=100)
    workflow_id = models.BigIntegerField('关联业务id')
    workflow_type = models.IntegerField('申请类型', choices=workflow_type_choices)
    workflow_title = models.CharField('申请标题', max_length=50)
    workflow_remark = models.CharField('申请备注', default='', max_length=140, blank=True)
    audit_auth_groups = models.CharField('审批权限组列表', max_length=255)
    current_audit = models.CharField('当前审批权限组', max_length=20)
    next_audit = models.CharField('下级审批权限组', max_length=20)
    current_status = models.IntegerField('审核状态', choices=workflow_status_choices)
    create_user = models.CharField('申请人', max_length=30)
    create_user_display = models.CharField('申请人中文名', max_length=50, default='')
    create_time = models.DateTimeField('申请时间', auto_now_add=True)
    sys_time = models.DateTimeField('系统时间', auto_now=True)

    def __int__(self):
        return self.audit_id

    class Meta:
        managed = True
        db_table = 'workflow_audit'
        unique_together = ('workflow_id', 'workflow_type')
        verbose_name = u'工作流审批列表'
        verbose_name_plural = u'工作流审批列表'


class WorkflowAuditDetail(models.Model):
    """
    审批明细表
    """
    audit_detail_id = models.AutoField(primary_key=True)
    audit_id = models.IntegerField('审核主表id')
    audit_user = models.CharField('审核人', max_length=30)
    audit_time = models.DateTimeField('审核时间')
    audit_status = models.IntegerField('审核状态', choices=workflow_status_choices)
    remark = models.CharField('审核备注', default='', max_length=140)
    sys_time = models.DateTimeField('系统时间', auto_now=True)

    def __int__(self):
        return self.audit_detail_id

    class Meta:
        managed = True
        db_table = 'workflow_audit_detail'
        verbose_name = u'工作流审批明细'
        verbose_name_plural = u'工作流审批明细'


class WorkflowAuditSetting(models.Model):
    """
    审批配置表
    """
    audit_setting_id = models.AutoField(primary_key=True)
    group_id = models.IntegerField('组ID')
    group_name = models.CharField('组名称', max_length=100)
    workflow_type = models.IntegerField('审批类型', choices=workflow_type_choices)
    audit_auth_groups = models.CharField('审批权限组列表', max_length=255)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    def __int__(self):
        return self.audit_setting_id

    class Meta:
        managed = True
        db_table = 'workflow_audit_setting'
        unique_together = ('group_id', 'workflow_type')
        verbose_name = u'审批流程配置'
        verbose_name_plural = u'审批流程配置'


class WorkflowLog(models.Model):
    """
    工作流日志表
    """
    id = models.AutoField(primary_key=True)
    audit_id = models.IntegerField('工单审批id', db_index=True)
    operation_type = models.SmallIntegerField('操作类型，0提交/待审核、1审核通过、2审核不通过、3审核取消、4定时、5执行、6执行结束')
    operation_type_desc = models.CharField('操作类型描述', max_length=10)
    operation_info = models.CharField('操作信息', max_length=200)
    operator = models.CharField('操作人', max_length=30)
    operator_display = models.CharField('操作人中文名', max_length=50, default='')
    operation_time = models.DateTimeField(auto_now_add=True)

    def __int__(self):
        return self.audit_id

    class Meta:
        managed = True
        db_table = 'workflow_log'
        verbose_name = u'工作流日志'
        verbose_name_plural = u'工作流日志'


class QueryPrivilegesApply(models.Model):
    """
    查询权限申请记录表
    """
    apply_id = models.AutoField(primary_key=True)
    group_id = models.IntegerField('组ID')
    group_name = models.CharField('组名称', max_length=100)
    title = models.CharField('申请标题', max_length=50)
    # TODO user_name display 改为外键
    user_name = models.CharField('申请人', max_length=30)
    user_display = models.CharField('申请人中文名', max_length=50, default='')
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    db_list = models.TextField('数据库', default='')  # 逗号分隔的数据库列表
    table_list = models.TextField('表', default='')  # 逗号分隔的表列表
    valid_date = models.DateField('有效时间')
    limit_num = models.IntegerField('行数限制', default=100)
    priv_type = models.IntegerField('权限类型', choices=((1, 'DATABASE'), (2, 'TABLE'),), default=0)
    status = models.IntegerField('审核状态', choices=workflow_status_choices)
    audit_auth_groups = models.CharField('审批权限组列表', max_length=255)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    def __int__(self):
        return self.apply_id

    class Meta:
        managed = True
        db_table = 'query_privileges_apply'
        verbose_name = u'查询权限申请记录表'
        verbose_name_plural = u'查询权限申请记录表'


class QueryPrivileges(models.Model):
    """
    用户权限关系表
    """
    privilege_id = models.AutoField(primary_key=True)
    user_name = models.CharField('用户名', max_length=30)
    user_display = models.CharField('申请人中文名', max_length=50, default='')
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    db_name = models.CharField('数据库', max_length=64, default='')
    table_name = models.CharField('表', max_length=64, default='')
    valid_date = models.DateField('有效时间')
    limit_num = models.IntegerField('行数限制', default=100)
    priv_type = models.IntegerField('权限类型', choices=((1, 'DATABASE'), (2, 'TABLE'),), default=0)
    is_deleted = models.IntegerField('是否删除', default=0)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    def __int__(self):
        return self.privilege_id

    class Meta:
        managed = True
        db_table = 'query_privileges'
        index_together = ["user_name", "instance", "db_name", "valid_date"]
        verbose_name = u'查询权限记录'
        verbose_name_plural = u'查询权限记录'


class QueryLog(models.Model):
    """
    记录在线查询sql的日志
    """
    # TODO 改为实例外键
    instance_name = models.CharField('实例名称', max_length=50)
    db_name = models.CharField('数据库名称', max_length=64)
    sqllog = models.TextField('执行的sql查询')
    effect_row = models.BigIntegerField('返回行数')
    cost_time = models.CharField('执行耗时', max_length=10, default='')
    # TODO 改为user 外键
    username = models.CharField('操作人', max_length=30)
    user_display = models.CharField('操作人中文名', max_length=50, default='')
    priv_check = models.BooleanField('查询权限是否正常校验', choices=((False, '跳过'), (True, '正常'),), default=False)
    hit_rule = models.BooleanField('查询是否命中脱敏规则', choices=((False, '未命中/未知'), (True, '命中')), default=False)
    masking = models.BooleanField('查询结果是否正常脱敏', choices=((False, '否'), (True, '是'),), default=False)
    create_time = models.DateTimeField('操作时间', auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = 'query_log'
        verbose_name = u'查询日志'
        verbose_name_plural = u'查询日志'


rule_type_choices = ((1, '手机号'), (2, '证件号码'), (3, '银行卡'), (4, '邮箱'), (5, '金额'), (6, '其他'))


class DataMaskingColumns(models.Model):
    """
    脱敏字段配置
    """
    column_id = models.AutoField('字段id', primary_key=True)
    rule_type = models.IntegerField('规则类型', choices=rule_type_choices)
    active = models.BooleanField('激活状态', choices=((False, '未激活'), (True, '激活')))
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    table_schema = models.CharField('字段所在库名', max_length=64)
    table_name = models.CharField('字段所在表名', max_length=64)
    column_name = models.CharField('字段名', max_length=64)
    column_comment = models.CharField('字段描述', max_length=1024, default='', blank=True)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = 'data_masking_columns'
        verbose_name = u'脱敏字段配置'
        verbose_name_plural = u'脱敏字段配置'


class DataMaskingRules(models.Model):
    """
    脱敏规则配置
    """
    rule_type = models.IntegerField('规则类型', choices=rule_type_choices, unique=True)
    rule_regex = models.CharField('规则脱敏所用的正则表达式，表达式必须分组，隐藏的组会使用****代替', max_length=255)
    hide_group = models.IntegerField('需要隐藏的组')
    rule_desc = models.CharField('规则描述', max_length=100, default='', blank=True)
    sys_time = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = 'data_masking_rules'
        verbose_name = u'脱敏规则配置'
        verbose_name_plural = u'脱敏规则配置'


class ParamTemplate(models.Model):
    """
    实例参数模板配置
    """
    db_type = models.CharField('数据库类型', max_length=20, choices=DB_TYPE_CHOICES)
    variable_name = models.CharField('参数名', max_length=64)
    default_value = models.CharField('默认参数值', max_length=1024)
    editable = models.BooleanField('是否支持修改', default=False)
    valid_values = models.CharField('有效参数值，范围参数[1-65535]，值参数[ON|OFF]', max_length=1024, blank=True)
    description = models.CharField('参数描述', max_length=1024, blank=True)
    create_time = models.DateTimeField('创建时间', auto_now_add=True)
    sys_time = models.DateTimeField('系统时间修改', auto_now=True)

    class Meta:
        managed = True
        db_table = 'param_template'
        unique_together = ('db_type', 'variable_name')
        verbose_name = u'实例参数模板配置'
        verbose_name_plural = u'实例参数模板配置'


class ParamHistory(models.Model):
    """
    可在线修改的动态参数配置
    """
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    variable_name = models.CharField('参数名', max_length=64)
    old_var = models.CharField('修改前参数值', max_length=1024)
    new_var = models.CharField('修改后参数值', max_length=1024)
    set_sql = models.CharField('在线变更配置执行的SQL语句', max_length=1024)
    user_name = models.CharField('修改人', max_length=30)
    user_display = models.CharField('修改人中文名', max_length=50)
    create_time = models.DateTimeField('参数被修改时间点', auto_now_add=True)

    class Meta:
        managed = True
        ordering = ['-create_time']
        db_table = 'param_history'
        verbose_name = u'实例参数修改历史'
        verbose_name_plural = u'实例参数修改历史'


class Config(models.Model):
    """
    配置信息表
    """
    item = models.CharField('配置项', max_length=50, primary_key=True)
    value = models.CharField('配置项值', max_length=200)
    description = models.CharField('描述', max_length=200, default='', blank=True)

    class Meta:
        managed = True
        db_table = 'sql_config'
        verbose_name = u'系统配置'
        verbose_name_plural = u'系统配置'


class AliyunAccessKey(models.Model):
    """
    记录阿里云的认证信息
    """
    ak = models.CharField(max_length=50)
    secret = models.CharField(max_length=100)
    is_enable = models.BooleanField('是否启用', default=True)
    remark = models.CharField(max_length=50, default='', blank=True)

    @property
    def raw_ak(self):
        """ 返回明文ak str """
        pc = Prpcrypt()  # 初始化
        return pc.decrypt(self.ak)

    @property
    def raw_secret(self):
        """ 返回明文secret str """
        pc = Prpcrypt()  # 初始化
        return pc.decrypt(self.secret)

    class Meta:
        managed = True
        db_table = 'aliyun_access_key'
        verbose_name = u'阿里云认证信息'
        verbose_name_plural = u'阿里云认证信息'

    def save(self, *args, **kwargs):
        pc = Prpcrypt()  # 初始化
        if self.id:
            old_info = AliyunAccessKey.objects.get(id=self.id)
            old_ak = old_info.ak
            old_secret = old_info.secret
        else:
            old_ak = ''
            old_secret = ''
        # 加密信息有变动才再次加密保存
        self.ak = pc.encrypt(self.ak) if old_ak != self.ak else self.ak
        self.secret = pc.encrypt(self.secret) if old_secret != self.secret else self.secret
        super(AliyunAccessKey, self).save(*args, **kwargs)


class AliyunRdsConfig(models.Model):
    """
    阿里云rds配置信息
    """
    instance = models.OneToOneField(Instance, on_delete=models.CASCADE)
    rds_dbinstanceid = models.CharField('对应阿里云RDS实例ID', max_length=100)
    is_enable = models.BooleanField('是否启用', default=True)

    def __int__(self):
        return self.rds_dbinstanceid

    class Meta:
        managed = True
        db_table = 'aliyun_rds_config'
        verbose_name = u'阿里云rds配置'
        verbose_name_plural = u'阿里云rds配置'


class Permission(models.Model):
    """
    自定义业务权限
    """

    class Meta:
        managed = True
        permissions = (
            ('menu_dashboard', '菜单 Dashboard'),
            ('menu_sqlcheck', '菜单 SQL审核'),
            ('menu_sqlworkflow', '菜单 SQL上线'),
            ('menu_sqlanalyze', '菜单 SQL分析'),
            ('menu_query', '菜单 SQL查询'),
            ('menu_sqlquery', '菜单 在线查询'),
            ('menu_queryapplylist', '菜单 权限管理'),
            ('menu_sqloptimize', '菜单 SQL优化'),
            ('menu_sqladvisor', '菜单 优化工具'),
            ('menu_slowquery', '菜单 慢查日志'),
            ('menu_instance', '菜单 实例管理'),
            ('menu_instance_list', '菜单 实例列表'),
            ('menu_dbdiagnostic', '菜单 会话管理'),
            ('menu_param', '菜单 参数配置'),
            ('menu_data_dictionary', '菜单 数据字典'),
            ('menu_binlog2sql', '菜单 Binlog2SQL'),
            ('menu_schemasync', '菜单 SchemaSync'),
            ('menu_system', '菜单 系统管理'),
            ('menu_document', '菜单 相关文档'),
            ('menu_themis', '菜单 数据库审核'),
            ('sql_submit', '提交SQL上线工单'),
            ('sql_review', '审核SQL上线工单'),
            ('sql_execute_for_resource_group', '执行SQL上线工单(资源组粒度)'),
            ('sql_execute', '执行SQL上线工单(仅自己提交的)'),
            ('sql_analyze', '执行SQL分析'),
            ('optimize_sqladvisor', '执行SQLAdvisor'),
            ('optimize_sqltuning', '执行SQLTuning'),
            ('optimize_soar', '执行SOAR'),
            ('query_applypriv', '申请查询权限'),
            ('query_mgtpriv', '管理查询权限'),
            ('query_review', '审核查询权限'),
            ('query_submit', '提交SQL查询'),
            ('query_all_instances', '可查询所有实例'),
            ('process_view', '查看会话'),
            ('process_kill', '终止会话'),
            ('tablespace_view', '查看表空间'),
            ('trxandlocks_view', '查看锁信息'),
            ('param_view', '查看实例参数列表'),
            ('param_edit', '修改实例参数'),
        )


class SlowQuery(models.Model):
    """
    SlowQuery
    """
    checksum = models.CharField(max_length=32, primary_key=True)
    sample = models.TextField()
    first_seen = models.DateTimeField(blank=True, null=True, auto_now_add=True)
    last_seen = models.DateTimeField(blank=True, null=True, db_index=True, auto_now=True)
    #fingerprint = models.TextField()
    #reviewed_by = models.CharField(max_length=20, blank=True, null=True)
    #reviewed_on = models.DateTimeField(blank=True, null=True)
    #comments = models.TextField(blank=True, null=True)


    class Meta:
        managed = True
        db_table = 'slow_query'
        verbose_name = u'慢日志统计'
        verbose_name_plural = u'慢日志统计'


class SlowQueryDetail(models.Model):
    checksum = models.ForeignKey(SlowQuery, db_constraint=False, to_field='checksum', db_column='checksum',
                                 on_delete=models.CASCADE)
    InstanceName = models.CharField(max_length=64, null=False, verbose_name='实例名称')
    DBName = models.CharField(max_length=64, null=False, verbose_name='数据库名')
    QueryTimes = models.FloatField(null=False, verbose_name='该语句的执行时长，单位为秒')
    ExecutionStartTime = models.DateTimeField(verbose_name='操作执行的开始时间')
    SQLText = models.TextField(max_length=1000, null=False, verbose_name='慢操作执行的语句')
    HostAddress = models.CharField(max_length=64, verbose_name='连接数据库的主机地址')
    ReturnRowCounts = models.IntegerField(verbose_name='返回行数')

    ParseRowCounts = models.FloatField(blank=True, null=True, verbose_name='解析行数')
    KeysExamined = models.IntegerField(blank=True, null=True, verbose_name='索引扫描行数')
    AccountName = models.CharField(max_length=64, blank=True, null=True, verbose_name='执行该操作的数据库用户名')
    DocsExamined = models.CharField(max_length=64, blank=True, null=True, verbose_name='该操作执行时扫描的文档数')

    def __str__(self):
        return self.DBName + ": " + self.SQLText

    class Meta:
        managed = True
        db_table = 'slow_query_detail'
        verbose_name = '慢日志明细'