FROM python:3.11-slim

WORKDIR /app/services/deployment-service

COPY services/deployment-service/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/deployment-service/ /app/services/deployment-service/
COPY shared/ /app/shared/

ENV PYTHONPATH=/app

CMD ["python", "main.py"]
