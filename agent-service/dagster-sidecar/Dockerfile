FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml .
RUN uv sync --all-extras --no-dev

COPY server.py .

EXPOSE 8112

CMD ["uv", "run", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8112"]
