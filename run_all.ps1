# run_all.ps1 - activate venv, run ETL, start API
Write-Host "Activating venv..."
& .\venv\Scripts\Activate.ps1

Write-Host "Running ETL (main.py)..."
python main.py
if ($LASTEXITCODE -ne 0) {
  Write-Host "ETL failed. Aborting." -ForegroundColor Red
  exit $LASTEXITCODE
}

Write-Host "Starting API. Press Ctrl+C to stop."
python -m uvicorn api.app:app --reload --port 8000
