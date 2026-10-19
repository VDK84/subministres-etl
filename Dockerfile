FROM python:3.11-slim

# Evitar prompts interactius i escriure bytecode
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

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
RUN pip install --no-cache-dir -r requirements.txt

# Instal·lar navegador headless per a scrapers (Chromium) amb dependències del sistema
RUN playwright install --with-deps chromium

# Copiar codi font
COPY src/ ./src/
COPY pyproject.toml .

# Crear directori de dades per al volum NAS
RUN mkdir -p /data/factures && chmod 777 /data/factures

ENTRYPOINT ["python", "-m", "src.main"]
CMD ["--days-back", "60"]
