FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN #apt-get update && apt-get install -y \
#    build-essential \
#    gcc \
#    libpq-dev \
#    gdal-bin \
#    libgdal-dev \
#    curl \
#    && rm -rf /var/lib/apt/lists/*

#ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
#ENV C_INCLUDE_PATH=/usr/include/gdal

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd -m appuser
USER appuser

# 👇 Adjust THIS depending on your structure
CMD ["python", "-m", "core.application"]