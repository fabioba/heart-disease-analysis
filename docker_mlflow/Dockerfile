FROM python:3.7

# Create app directory
WORKDIR /app

# Install app dependencies
COPY docker_mlflow/requirements.txt .

RUN pip install -r requirements.txt

# Bundle app source
COPY . .

EXPOSE 600


CMD ["mlflow", "server", "--backend-store-uri", "postgresql://airflow:airflow@postgres:5432/mlflowdb", "--default-artifact-root", "./mlruns", "-h", "0.0.0.0", "-p", "600"]