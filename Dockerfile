FROM python:3.9-slim-buster

WORKDIR /src/

ADD requirements.txt /src/
RUN pip install -r requirements.txt

ADD fetchers /src/fetchers
ADD config /src/config
ADD tests /src/tests
ADD setup.py /src/
RUN python setup.py install
