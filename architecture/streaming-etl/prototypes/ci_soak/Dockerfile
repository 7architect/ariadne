FROM ghcr.io/astral-sh/uv@sha256:61d393e44e249f2e4b526b6c7ddcecce245946826e608e11c93ad4f5bba55b21 AS uv
FROM python@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704

COPY --from=uv /uv /uvx /bin/

WORKDIR /app/ci_soak
COPY ci_soak/pyproject.toml ci_soak/uv.lock ./
RUN --mount=type=secret,id=proxy_ca,required=false \
    if [ -f /run/secrets/proxy_ca ]; then \
      UV_NATIVE_TLS=true SSL_CERT_FILE=/run/secrets/proxy_ca \
        uv sync --frozen --no-dev --no-install-project; \
    else \
      uv sync --frozen --no-dev --no-install-project; \
    fi
ENV PYTHONPATH=/app PATH=/app/ci_soak/.venv/bin:$PATH
WORKDIR /app
COPY postgres_*.py /app/
COPY ci_soak/*.py /app/ci_soak/

ARG SOAK_SOURCE_DIGEST
ARG SOAK_CORE_DIGEST
LABEL ariadne.soak.source_digest=${SOAK_SOURCE_DIGEST} \
      ariadne.soak.core_digest=${SOAK_CORE_DIGEST}

CMD ["python", "/app/ci_soak/supervisor.py"]
