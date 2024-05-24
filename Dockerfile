FROM python:3.8

COPY . /app
WORKDIR ./app

RUN pip install -r requirements.txt
RUN /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' >/etc/timezone

EXPOSE 5000

ENTRYPOINT ["python"]
CMD ["app.py"]