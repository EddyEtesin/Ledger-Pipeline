FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY silver_consumer.py ./

CMD ["python", "-u", "silver_consumer.py"]
