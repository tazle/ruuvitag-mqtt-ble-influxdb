FROM python:3.9

COPY requirements.txt /
#RUN pip install --no-deps ruuvitag-sensor &&
RUN pip install -r requirements.txt

ENV PYTHONUNBUFFERED=1
COPY mqtt-to-influx.py /
COPY test-mac-mapping.json /

