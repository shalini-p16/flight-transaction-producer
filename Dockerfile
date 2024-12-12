# Base Python image
FROM python:3.9-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set a working directory in the container
WORKDIR /app

COPY app ./

# Copy the requirements file into the container
COPY requirements.txt ./

# Install dependencies
RUN pip install -r /app/requirements.txt

RUN ls -R
# Command to run the ingestion script
CMD ["python", "main.py"]

