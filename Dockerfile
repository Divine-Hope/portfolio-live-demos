# One image for every Python service (ingest, api, archive, migrate, the fake stream);
# compose picks the command.
# Multi-stage: build the virtualenv with uv, ship only the venv on a slim base.

FROM python:3.12-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:3.12-slim
RUN useradd --system --uid 10001 --home /app app
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
USER app
EXPOSE 8000 9101
CMD ["uvicorn", "livedemos.api.app:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
