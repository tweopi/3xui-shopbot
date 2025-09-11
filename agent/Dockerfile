FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Install Ookla speedtest CLI
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget tar \
    && update-ca-certificates \
    && wget -O /tmp/ookla-speedtest.tgz https://install.speedtest.net/app/cli/ookla-speedtest-1.2.0-linux-x86_64.tgz \
    && mkdir -p /tmp/ookla \
    && tar -xzf /tmp/ookla-speedtest.tgz -C /tmp/ookla \
    && if [ -f /tmp/ookla/speedtest ]; then mv /tmp/ookla/speedtest /usr/local/bin/speedtest; \
       elif [ -f /tmp/ookla/ookla-speedtest-1.2.0-linux-x86_64/speedtest ]; then mv /tmp/ookla/ookla-speedtest-1.2.0-linux-x86_64/speedtest /usr/local/bin/speedtest; \
       else echo "speedtest binary not found in archive" && exit 1; fi \
    && chmod +x /usr/local/bin/speedtest \
    && ln -sf /usr/local/bin/speedtest /usr/bin/speedtest \
    && rm -rf /tmp/ookla /tmp/ookla-speedtest.tgz \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
RUN pip install --no-cache-dir Flask==3.1.1

COPY app.py /app/app.py
EXPOSE 8080
ENV PORT=8080
# Set token by ENV at run-time: -e AGENT_TOKEN=...
CMD ["python", "app.py"]
