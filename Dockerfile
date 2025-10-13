# Dockerfile
FROM apache/airflow:3.1.0-python3.10

# Copiar requirements
COPY requirements.txt /requirements.txt

# Instalar dependências
RUN pip install --no-cache-dir -r /requirements.txt

# Copiar código fonte
COPY src/ /opt/airflow/src/
