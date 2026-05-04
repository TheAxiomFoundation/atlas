# Contributing

Use short-lived branches off `main` and open a pull request back to `main`.
Keep PRs focused, describe the checks you ran, and wait for CI before merging.

## Pull request flow

1. Create a branch from an up-to-date `main`.
2. Make the smallest coherent change and include tests for behavior changes.
3. Add a Towncrier fragment under `changelog.d/` unless the PR is docs,
   tests-only, or otherwise has no user-visible release note.
4. Open the PR to `main` and complete the PR template.
5. Merge after review approval and green CI.

Towncrier fragment categories are `breaking`, `added`, `changed`, `fixed`, and
`removed`. Name fragments descriptively, for example
`changelog.d/historical-versioning.fixed.md`.

## Local checks

CI runs the changelog draft, Ruff, mypy, tests with coverage, and a PostgreSQL
smoke check. Run the relevant subset locally before opening the PR:

```bash
python -m pip install -e ".[dev]"
python -m pip install pytest-cov pytest-timeout
python -m towncrier build --draft --version 0.0.0
ruff check .
mypy src/axiom_corpus/corpus --ignore-missing-imports
pytest -v --cov=axiom_corpus --cov-report=term-missing --cov-config=pyproject.toml --timeout=60
```

## Repo notes

- Generated `data/` and `sources/` files are local artifacts; do not commit them
  unless the PR explicitly adds a fixture or catalog source.
- Integration tests and live-source fetches should be isolated from the default
  offline test path.
- Keep storage, fetcher, and API changes covered by focused tests because they
  affect downstream encode and app workflows.
