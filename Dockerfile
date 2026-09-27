FROM python:3.11-slim

WORKDIR /app

# Install deps first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Set encoding to avoid terminal rendering issues with box-drawing characters
ENV PYTHONIOENCODING=utf-8

# All secrets/config come from the environment or a mounted .env file.
# Default DB path can be overridden with LEAD_DISPATCH_DB.
COPY . .

ENTRYPOINT ["python", "lead_dispatch_system.py"]
CMD ["--help"]
