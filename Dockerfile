# 1. Usa una versión específica de Python para mayor reproducibilidad
FROM python:3.11.9-slim

# 2. Variables de entorno al principio
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# 3. Instala todas las dependencias del sistema y Chrome en una sola capa para reducir el tamaño
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    curl \
    gnupg \
    xvfb \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libxss1 \
    libnss3 \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    # Limpieza
    && apt-get purge -y --auto-remove wget \
    && rm -rf /var/lib/apt/lists/*

# 4. Crea el usuario y el directorio de la aplicación
#    Crea los subdirectorios aquí, mientras eres root
RUN useradd --create-home --shell /bin/bash scraper && \
    mkdir -p /app/downloads /app/logs && \
    chown -R scraper:scraper /app

# 5. Establece el directorio de trabajo
WORKDIR /app

# 6. Copia solo requirements.txt para aprovechar la caché de Docker
COPY --chown=scraper:scraper requirements.txt .

# 7. Instala las dependencias de Python.
#    Ya no es necesario ejecutar como root, pero pip gestiona bien sus directorios.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 8. Copia el resto del código de la aplicación
COPY --chown=scraper:scraper . .

# 9. Cambia al usuario sin privilegios para la ejecución
USER scraper

# 10. Expón el puerto
EXPOSE 8000

# 11. El HEALTHCHECK y CMD no necesitan cambios
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
