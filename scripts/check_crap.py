"""Fail when a covered source function has a CRAP score of six or more."""

from __future__ import annotations

import sys
from pathlib import Path

from coverage import Coverage
from radon.complexity import cc_visit
from radon.visitors import Function

THRESHOLD = 6.0
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"


def _source_files() -> tuple[Path, ...]:
    return tuple(sorted(SOURCE_ROOT.rglob("*.py"), key=lambda path: str(path)))


def _coverage_data() -> Coverage:
    coverage = Coverage(data_file=str(PROJECT_ROOT / ".coverage"))
    try:
        coverage.load()
    except Exception as exc:  # pragma: no cover - environment-specific coverage errors
        raise RuntimeError("coverage data is missing; run pytest with coverage first") from exc
    return coverage


def _function_coverage(coverage: Coverage, path: Path, start: int, end: int) -> float:
    _, statements, _, missing, _ = coverage.analysis2(str(path))
    function_statements = {line for line in statements if start <= line <= end}
    if not function_statements:
        return 1.0
    function_missing = {line for line in missing if line in function_statements}
    return (len(function_statements) - len(function_missing)) / len(function_statements)


def _crap_score(complexity: int, coverage: float) -> float:
    uncovered = 1.0 - coverage
    return complexity**2 * uncovered**3 + complexity


def _scores(coverage: Coverage) -> list[tuple[str, int, float, float]]:
    scores: list[tuple[str, int, float, float]] = []
    for path in _source_files():
        source = path.read_text(encoding="utf-8")
        for function in cc_visit(source):
            if not isinstance(function, Function):
                continue
            function_coverage = _function_coverage(
                coverage, path, function.lineno, function.endline
            )
            scores.append(
                (
                    f"{path.relative_to(PROJECT_ROOT)}:{function.lineno}:{function.name}",
                    function.complexity,
                    function_coverage,
                    _crap_score(function.complexity, function_coverage),
                )
            )
    return scores


def main() -> int:
    """Print function scores and return non-zero for a CRAP violation."""

    try:
        scores = _scores(_coverage_data())
    except (OSError, RuntimeError, SyntaxError, ValueError) as exc:
        print(f"CRAP check failed: {exc}", file=sys.stderr)
        return 1
    violations = 0
    for name, complexity, coverage, score in scores:
        print(f"{name} complexity={complexity} coverage={coverage:.3f} crap={score:.3f}")
        if score >= THRESHOLD:
            violations += 1
    if violations:
        print(f"CRAP check failed: {violations} function(s) have score >= {THRESHOLD:g}")
        return 1
    print(f"CRAP check passed: {len(scores)} functions below {THRESHOLD:g}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
