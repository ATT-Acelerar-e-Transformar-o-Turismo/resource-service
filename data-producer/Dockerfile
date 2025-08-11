FROM python:3.13-slim

WORKDIR /app

RUN pip install pika requests

COPY . .

CMD ["python", "data_producer.py"]
