FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bronze_consumer.py ./

CMD ["python", "-u", "bronze_consumer.py"]
