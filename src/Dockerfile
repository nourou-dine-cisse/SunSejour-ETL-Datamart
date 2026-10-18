FROM python:3.11-slim

# Dépendances système pour pymssql (FreeTDS)
RUN apt-get update && apt-get install -y \
    freetds-dev \
    freetds-bin \
    gcc \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

CMD ["./entrypoint.sh"]
