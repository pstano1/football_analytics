FROM python:3.13-slim

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY dagster.yaml workspace.yaml . 
COPY dags ./dags

EXPOSE 3000

CMD ["dagster-webserver", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]
