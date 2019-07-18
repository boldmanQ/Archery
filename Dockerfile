FROM python:3.6-slim-jessie
COPY . /usr/src/app
WORKDIR /usr/src/app
RUN pip install -r requirements.txt
EXPOSE 8000
CMD ["gunicorn", "-c", "gunicorn_conf.py", "archery.wsgi:application"]
