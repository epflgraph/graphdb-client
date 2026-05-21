# Pin the Python base image by digest for reproducible builds.
FROM python@sha256:9bffe4353b925a1656688797ebc68f9c525e79b1d377a764d232182a519eeec4

# Prevent Python from writing .pyc files and force unbuffered stdout/stderr.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Use /app as the application working directory.
WORKDIR /app

# MySQL CLI tools are required by export/import code paths.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy packaging metadata first so dependency installation can be cached
# when only application source code changes.
COPY pyproject.toml README.md LICENSE /app/

# Install project dependencies before copying source code to improve layer caching.
RUN pip install --no-cache-dir .

# Copy the example config separately because it changes less often than source code.
COPY config.example.yaml /app/config.example.yaml

# Create a safe default config so the CLI imports cleanly.
# Mount a real config at runtime: -v $(pwd)/config.yaml:/app/config.yaml:ro
RUN cp /app/config.example.yaml /app/config.yaml

# Copy application source code near the end so code edits invalidate fewer layers.
COPY graphdb /app/graphdb

# Install the package itself after copying source code.
RUN pip install --no-cache-dir .

# The graphdb CLI is the default entrypoint, and it will print usage information if no arguments are provided.
ENTRYPOINT ["graphdb"]
CMD ["-h"]
