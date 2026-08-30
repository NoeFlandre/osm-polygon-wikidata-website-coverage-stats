set shell := ["zsh", "-eu -o pipefail", "-c"]

cache := "/Volumes/Seagate M3/projects/osm-polygon-wikidata-website-coverage-stats/.uv-cache"

sync:
	UV_CACHE_DIR={{cache}} uv sync --frozen --offline

test:
	UV_CACHE_DIR={{cache}} uv run --offline pytest --cov=src/osm_polygon_wikidata_website_coverage --cov-branch --cov-fail-under=100

lint:
	UV_CACHE_DIR={{cache}} uv run --offline ruff format --check .
	UV_CACHE_DIR={{cache}} uv run --offline ruff check .

typecheck:
	UV_CACHE_DIR={{cache}} uv run --offline ty check src tests

docs:
	UV_CACHE_DIR={{cache}} uv run --offline mkdocs build --strict

crap:
	UV_CACHE_DIR={{cache}} uv run --offline python scripts/check_crap.py

mutation:
	UV_CACHE_DIR={{cache}} uv run --offline mutmut run --max-children 2

qa: lint typecheck test docs crap mutation
