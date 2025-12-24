FROM python:alpine

WORKDIR /app

COPY shared/ /app/shared/

RUN pip install --no-cache-dir \
    -r /app/shared/requirements.txt \
    websockets>=11

COPY ingester/ingester.py .

ENV PYTHONPATH=/app/shared

CMD ["python", "ingester.py"]
