FROM python:3.11-slim

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app

COPY requirements.txt .

RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

COPY dagster.yaml workspace.yaml .
COPY dags ./dags
COPY jobs ./jobs

EXPOSE 3000

CMD ["dagster-webserver", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]
