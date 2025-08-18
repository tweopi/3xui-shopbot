FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
RUN python3 -m venv .venv
ENV PATH="/app/.venv/bin:$PATH"
COPY . /app/project/
WORKDIR /app/project
RUN pip install --no-cache-dir -e .
CMD ["python3", "-m", "shop_bot"]