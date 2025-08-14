# Simple Document Fill App (Non-HIPAA)

Upload a `.zip` of PDFs and auto-fill fields from user input:
- Fills AcroForms when available.
- Falls back to overlay near label anchors ("name:", "email:", etc.).
- Returns a single ZIP of processed PDFs.

## Run locally
```bash
python -m venv .venv
. .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app/app.py
# open http://localhost:5000
```

## Customize
- Edit label variants in `app/config/patterns.yaml`.
- Edit field mapping in `app/config/mapping.yaml`.


## Make it a GitHub repository

```bash
# inside the project folder
git init
git add .
git commit -m "Initial commit: simple doc fill app"
git branch -M main
# create a new empty repo on GitHub named simple-doc-fill-app, then:
git remote add origin https://github.com/<your-username>/simple-doc-fill-app.git
git push -u origin main
```

GitHub Actions CI is included at `.github/workflows/ci.yml` (lint + smoke test).
