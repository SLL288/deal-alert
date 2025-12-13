# Real Estate Deal Alerts MVP (Static + JSON)

This MVP has **two parts**:

1) A Python collector script (runs on a schedule) that generates:
- `alerts.json`
- `top_deals.json`
- `last_run.json`

2) A static website (no backend) that reads those JSON files and displays deals:
- `index.html` (English)
- `index_zh.html` (中文)

## Quick start (local)
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/build_alerts.py --mode demo  # or --mode seed to use data/seeds.json
python -m http.server 8000
# open http://localhost:8000
```

### PowerShell setup (Windows)
```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## GitHub Pages
- Point Pages to deploy from the repository root so `index.html` and the JSON files are served.
- Ensure CI workflow commits the generated `*.json` after each run.
- Keep `data/app.sqlite` out of git (already ignored) to avoid leaking history; it will be regenerated on the runner.

## GitHub Actions (free scheduled runs)
Workflow: `.github/workflows/build-alerts.yml`

By default it runs in `demo` mode (safe). When you switch to a real source, change the workflow command
to `--mode live` and use the safe fetcher in `scripts/fetch_public.py` or a compliant partner feed.

> Start with demo mode to ship the pipeline today. Then add compliant data sources.

## Launch checklist
- Enable GitHub Pages to serve from the repo root and note the URL (typically `https://<org>.github.io/<repo>/`).
- Check Actions runs in GitHub → Actions → `Build deal alerts JSON`; confirm it succeeds and JSON files change (see “Last run” timestamp on the site).
- Share the Pages URL (English: `/index.html`, Chinese: `/index_zh.html`, Subscribe: `/subscribe.html`).
- Remember: current data is demo-only (no live crawling, no database). Switch the workflow command when ready for real sources.
- Seed mode: edit `data/seeds.json` (or use `admin_seeds.html` to copy JSON) and run `python scripts/build_alerts.py --mode seed`.
