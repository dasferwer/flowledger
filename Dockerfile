FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv && uv sync --frozen --no-dev
COPY flowledger ./flowledger
ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "-m", "flowledger.publisher"]
