FROM python:3.12-slim

WORKDIR /app

# Copy shared code
COPY shared/ /app/shared/

# Install shared deps only
RUN pip install --no-cache-dir -r /app/shared/requirements.txt

# Copy service code
COPY p2/consumer.py p2/recorder.py p2/forensics.py p2/p2.py ./

RUN mkdir -p /app/data

ENV PYTHONPATH=/app/shared

CMD ["python", "p2.py"]
