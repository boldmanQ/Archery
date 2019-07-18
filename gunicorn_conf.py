# RTFM -> http://docs.gunicorn.org/en/latest/settings.html#settings

bind = '0.0.0.0:8000'
workers = 4

timeout = 30

worker_class = 'gevent'

max_requests = 2000
max_requests_jitter = 500

proc_name = 'archery'

accesslog = '-'
errorlog = '-'
loglevel = 'info'
