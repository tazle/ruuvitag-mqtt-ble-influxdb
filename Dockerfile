FROM python:3.7-alpine

COPY requirements.txt /
RUN pip install --no-deps ruuvitag-sensor && pip install -r requirements.txt

ENV PYTHONUNBUFFERED=1
COPY mqtt-to-influx.py /

