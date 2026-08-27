set shell := ["zsh", "-eu -o pipefail", "-c"]

cache := "/private/tmp/osm-polygon-coverage-uv-cache"

sync:
	UV_CACHE_DIR={{cache}} uv sync

test:
	UV_CACHE_DIR={{cache}} uv run pytest --cov=src/osm_polygon_wikidata_website_coverage --cov-branch --cov-fail-under=100

lint:
	UV_CACHE_DIR={{cache}} uv run ruff format --check .
	UV_CACHE_DIR={{cache}} uv run ruff check .

typecheck:
	UV_CACHE_DIR={{cache}} uv run ty check src tests

docs:
	UV_CACHE_DIR={{cache}} uv run mkdocs build --strict

crap:
	UV_CACHE_DIR={{cache}} uv run python scripts/check_crap.py

mutation:
	UV_CACHE_DIR={{cache}} uv run mutmut run --max-children 2

qa: sync lint typecheck test docs crap mutation
