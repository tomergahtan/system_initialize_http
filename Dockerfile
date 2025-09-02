# Use official Python 3.12 slim base image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install pip dependencies first (leveraging Docker cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Set default command to run your module entrypoint
CMD ["uvicorn", "index:app", "--host", "0.0.0.0", "--port", "8000"]
