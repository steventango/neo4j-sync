FROM python:3.12

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt
