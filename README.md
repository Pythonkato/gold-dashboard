# Gold Macro Dashboard

This repository hosts a static dashboard that visualizes gold-market and macroeconomic time series. The site is meant to be deployed with GitHub Pages and updated automatically via GitHub Actions.

## Data refresh workflow

- `fetch_data.py` downloads the latest data from FRED and Alpha Vantage, then normalizes each series into JSON files under `data/` for the dashboard to consume.
- `.github/workflows/fetch.yml` runs the fetcher every day at 03:00 UTC, committing updated data files when changes are detected.

## API keys and repository secrets

The project does **not** hard-code any API credentials. The fetcher reads its credentials from environment variables so that GitHub Actions can supply them securely at runtime. Store your keys as repository secrets (Settings → Secrets and variables → Actions) using the following names:

- `FRED_API_KEY`
- `ALPHAVANTAGE_API_KEY`
- (optional) `CB_SHEETS_CSV_URL`

GitHub stores these secrets encrypted at rest and only exposes them to the workflow runs where you explicitly reference them. Public repositories can safely use repository secrets because the values never appear in the build logs unless you print them yourself, and they are redacted automatically if accidentally echoed. As long as you do not commit the raw keys into the repository, other users cannot read them from your codebase or the Git history.

To test the fetcher locally, set the same environment variables before running the script, for example:

```bash
export FRED_API_KEY="your_fred_key"
export ALPHAVANTAGE_API_KEY="your_alpha_vantage_key"
python fetch_data.py
```

## Local development

Open `index.html` in a modern browser to view the dashboard, or use a simple HTTP server so that the page can fetch JSON files from `data/`.
