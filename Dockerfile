FROM python:3.11-slim

# Variables de entorno
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependencias básicas
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    xvfb \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Instalar Google Chrome (método actualizado)
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/googlechrome-linux-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/googlechrome-linux-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Crear usuario
RUN useradd --create-home --shell /bin/bash scraper

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .
RUN chown -R scraper:scraper /app

# Cambiar a usuario no-root
USER scraper

# Crear directorios
RUN mkdir -p /app/downloads /app/logs

# Puerto
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Comando
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
