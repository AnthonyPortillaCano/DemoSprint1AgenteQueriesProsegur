# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y build-essential

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy project
COPY . /app/

# Expose the port (FastAPI default: 8000)
EXPOSE 8000

# Command for FastAPI backend (adjust if needed)
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "src.main:app", "--bind=0.0.0.0:8000"]
