FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.13 /uv /usr/local/bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

# Primero solo las dependencias: esta capa se reutiliza mientras no cambie el lock.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY sql ./sql
RUN uv sync --frozen --no-dev

ENTRYPOINT ["uv", "run", "--no-sync", "jobs-pipeline"]
