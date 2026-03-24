# Single image used for both the api and producer services.
# The CMD is overridden per-service in docker-compose.yml.
FROM python:3.12-slim

# Bring in uv from the official distroless image — no pip needed.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # Compile .pyc files at install time so runtime startup is faster.
    UV_COMPILE_BYTECODE=1 \
    # Copy packages rather than symlinking — required inside Docker.
    UV_LINK_MODE=copy

WORKDIR /app

# ---- dependency layer (cached unless pyproject.toml / uv.lock changes) ----
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Activate the venv for all subsequent RUN / CMD / ENTRYPOINT steps.
ENV PATH="/app/.venv/bin:$PATH" \
    # Root of the project is on PYTHONPATH so `from shared.pii import ...` resolves.
    PYTHONPATH=/app

# ---- source ----
COPY . .
