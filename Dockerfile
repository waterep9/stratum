FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STRATUM_DATA_DIR=/var/lib/stratum

WORKDIR /app

COPY pyproject.toml ./
COPY stratum ./stratum
RUN pip install --no-cache-dir .

# 非 root 运行；数据目录提前创建并授权
RUN useradd --create-home --uid 10001 stratum \
    && mkdir -p /var/lib/stratum \
    && chown -R stratum:stratum /var/lib/stratum /app
USER stratum

EXPOSE 8080
VOLUME ["/var/lib/stratum"]

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).status==200 else 1)"

CMD ["uvicorn", "stratum.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080"]
