FROM python:3.12-slim

# Spark needs a JVM
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[iceberg,dev]"
COPY tests ./tests
COPY configs ./configs
COPY scripts ./scripts

ENV PYTHONUNBUFFERED=1
EXPOSE 9100
ENTRYPOINT ["crawlpipe"]
CMD ["run-all", "--out", "/data/run", "--pages", "600", "--warcs", "3"]
