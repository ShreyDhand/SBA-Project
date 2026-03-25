FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ensure the log and data directories exist if needed
RUN touch health_status.json && chmod 666 health_status.json

EXPOSE 8120

CMD ["python", "app.py"]