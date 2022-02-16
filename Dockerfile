#Dockerfile to demo download mysqlhandler from public github repo
# Julian Briggs
# 16-feb-2022

FROM python:3.9-alpine

#RUN git clone https://github.com/SheffieldSolar/mysqlhandler.git #Git not installed
#RUN pip install git+https://github.com/SheffieldSolar/mysqlhandler.git
#RUN pip install git
RUN apk add git
RUN git clone https://github.com/SheffieldSolar/mysqlhandler.git
