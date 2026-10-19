FROM python:3.11-slim

# Evitar prompts interactius i escriure bytecode
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    APP_HOST=0.0.0.0

WORKDIR /app

# Instal·lar paquets de sistema bàsics
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gcc \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements i instal·lar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && playwright install --with-deps chromium \
    && mkdir -p /data/factures && chmod 777 /data/factures

EXPOSE 8000

CMD ["uvicorn", "src.web.app:app", "--host", "0.0.0.0", "--port", "8000"]
