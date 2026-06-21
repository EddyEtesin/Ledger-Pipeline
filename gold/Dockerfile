FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gold_consumer.py ./

CMD ["python", "-u", "gold_consumer.py"]
