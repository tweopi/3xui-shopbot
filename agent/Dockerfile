FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Install Ookla speedtest CLI
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget tar \
    && update-ca-certificates \
    && wget -O /tmp/ookla-speedtest.tgz https://install.speedtest.net/app/cli/ookla-speedtest-1.2.0-linux-x86_64.tgz \
    && tar -xzf /tmp/ookla-speedtest.tgz -C /usr/local/bin/ \
    && chmod +x /usr/local/bin/speedtest \
    && rm -f /tmp/ookla-speedtest.tgz \
    && apt-get purge -y --auto-remove wget tar \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
RUN pip install --no-cache-dir Flask==3.1.1

COPY app.py /app/app.py
EXPOSE 8080
ENV PORT=8080
# Set token by ENV at run-time: -e AGENT_TOKEN=...
CMD ["python", "app.py"]
