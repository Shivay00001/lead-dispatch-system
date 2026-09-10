FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set encoding to avoid terminal rendering issues with box-drawing characters
ENV PYTHONIOENCODING=utf-8

COPY . .

ENTRYPOINT ["python", "lead_dispatch_system.py"]
CMD ["--help"]
