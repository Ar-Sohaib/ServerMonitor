FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY server.py poller.py agent.py ./
COPY static/ static/
COPY start.sh .
RUN chmod +x start.sh

# SSH keys are mounted at runtime — create the directory
RUN mkdir -p /keys

EXPOSE 8080

ENTRYPOINT ["./start.sh"]
