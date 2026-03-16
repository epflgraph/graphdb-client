FROM python:3.11.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# MySQL CLI tools are required by export/import code paths.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md LICENSE /app/
COPY graphdb /app/graphdb
COPY config.example.yaml /app/config.example.yaml

# Create a safe default config so the CLI imports cleanly.
# Mount a real config at runtime: -v $(pwd)/config.yaml:/app/config.yaml:ro
RUN cp /app/config.example.yaml /app/config.yaml \
    && pip install --no-cache-dir .

ENTRYPOINT ["graphdb"]
CMD ["-h"]
