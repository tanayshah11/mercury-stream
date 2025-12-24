FROM python:alpine

WORKDIR /app

COPY shared/ /app/shared/

RUN pip install --no-cache-dir -r /app/shared/requirements.txt

COPY processor/consumer.py processor/recorder.py processor/forensics.py processor/metrics.py processor/processor.py ./
COPY processor/incident/ /app/incident/

RUN mkdir -p /app/data

ENV PYTHONPATH=/app/shared

EXPOSE 9001 9090

CMD ["python", "processor.py"]
