FROM python:3.11

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY apps/control-plane /app/apps/control-plane
COPY packages /app/packages

WORKDIR /app/apps/control-plane

