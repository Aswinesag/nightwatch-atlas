FROM python:3.11
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
ENV KAFKA_BROKER=kafka:29092
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
CMD ["python", "main.py"]
