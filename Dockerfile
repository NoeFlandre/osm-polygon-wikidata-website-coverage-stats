FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml README.md LICENSE CITATION.cff ./
COPY src ./src

RUN python -m pip install --no-cache-dir .

ENTRYPOINT ["coverage"]
CMD ["--help"]
