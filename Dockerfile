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
    && apt-get purge -y --auto-remove gcc libffi-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Crear usuari no-root per seguretat (Principle of Least Privilege)
RUN groupadd -r -g 1000 appuser && \
    useradd -r -u 1000 -g appuser -d /app -s /sbin/nologin appuser && \
    mkdir -p /data/factures /ms-playwright && \
    chown -R appuser:appuser /app /data /ms-playwright

# Copiar el codi de l'aplicació
COPY --chown=appuser:appuser src/ ./src
ENV PYTHONPATH=/app

# Versió SemVer injectada durant el build
ARG APP_VERSION=0.15.0
ENV APP_VERSION=${APP_VERSION}

USER appuser

EXPOSE 8000

CMD ["uvicorn", "src.web.app:app", "--host", "0.0.0.0", "--port", "8000"]
