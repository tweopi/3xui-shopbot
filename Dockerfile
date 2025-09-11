FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
RUN python3 -m venv .venv
ENV PATH="/app/.venv/bin:$PATH"
ENV DEBIAN_FRONTEND=noninteractive
# Install dependencies and Ookla Speedtest CLI
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
COPY . /app/project/
WORKDIR /app/project
RUN pip install --no-cache-dir -e .
CMD ["python3", "-m", "shop_bot"]