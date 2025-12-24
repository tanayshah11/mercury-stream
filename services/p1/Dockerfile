FROM python:3.12-slim

WORKDIR /app

# copy shared code
COPY shared/ /app/shared/

# install shared and service-specific dependencies
RUN pip install --no-cache-dir \
    -r /app/shared/requirements.txt \
    websockets>=11

# copy service code
COPY p1/p1.py .

# set the python path
ENV PYTHONPATH=/app/shared

# run the service
CMD ["python", "p1.py"]
