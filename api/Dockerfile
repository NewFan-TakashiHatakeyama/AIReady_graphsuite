# Production Dockerfile
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install uv, a fast Python package installer
RUN pip install uv

# Copy the entire project
COPY . .

# Install the project along with its test dependencies for local validation
RUN uv pip install --system ".[test]"

# Expose the port the app runs on
EXPOSE 9621

# Run the application with gunicorn (production mode)
CMD ["python", "run_with_gunicorn.py"]
