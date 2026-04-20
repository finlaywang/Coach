# Repository Guidelines

## Project Structure & Module Organization
This repository is a lightweight Python data-processing project for OTA order normalization and daily summary output.

- `ota_daily_report.py`: Single OTA aggregation pipeline (KKday, Klook, GYG, Trip), Excel output and optional POST sender.
- `build_product_mapping.py`: Exports OTA product mapping from the Lion position-control Excel to `ota-mapping.csv`.
- `Downloads/`: Input source files (CSV/XLSX) matched by filename patterns.
- `requirements.txt`: Runtime dependencies.
- Root outputs: `ota_daily_sum_items.xlsx` (generated artifact).
- `OTA_DAILY_SUM_REQUIREMENTS.md`: Business and field-level requirements.

Keep new parsing logic in `scripts/` and avoid committing large raw export files unless required for reproducible debugging.

## Build, Test, and Development Commands
- `python3 -m venv .venv && source .venv/bin/activate`: Create and activate local environment.
- `pip install -r requirements.txt`: Install dependencies (`pandas`, `openpyxl`, `requests`).
- `python3 ota_daily_report.py --downloads-dir Downloads --verbose`: Run daily summary Excel pipeline.
- `python3 ota_daily_report.py --downloads-dir Downloads --dry-run`: Aggregate and write Excel only (no POST).
- `python3 ota_daily_report.py --downloads-dir Downloads --enable-post --endpoint <url>`: Send batched POST requests.

## Coding Style & Naming Conventions
- Python 3.10+ style with 4-space indentation and type hints where practical.
- Use `snake_case` for functions/variables and `UPPER_CASE` for constants.
- Keep parser functions platform-scoped (`parse_kkday`, `parse_klook`, etc.).
- Prefer small pure helpers (`to_int`, `to_date_str`, `normalize_text`) over duplicated inline conversion logic.

## Testing Guidelines
Automated tests are not present yet. Add `pytest` tests under `tests/` for all new parsing or classification logic.

- Test file pattern: `tests/test_<module>.py`
- Focus on edge cases: missing columns, malformed dates, mixed language labels, zero/negative traveler counts.
- For regression safety, include at least one fixture per OTA platform.

## Commit & Pull Request Guidelines
Git history is unavailable in this workspace snapshot, so no repository-specific pattern can be inferred. Use this default:

- Commit format: `type(scope): short imperative summary` (example: `feat(parser): add trip language fallback`).
- PRs should include: purpose, affected platforms, sample input filenames, and before/after output snippets.
- Link requirement updates when behavior changes (update `OTA_DAILY_SUM_REQUIREMENTS.md` in the same PR).

## Security & Configuration Tips
- Current script keeps a default Lark webhook for internal convenience; use `--lark-webhook` to override per run.
- Pass API endpoints via CLI flags (`--endpoint`) and avoid hardcoding new secrets in source files.
- Treat `Downloads/` as untrusted input; validate columns and date parsing defensively.
