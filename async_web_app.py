import csv
import uuid
from pathlib import Path
from typing import Dict, Any, Set

from fastapi import FastAPI, UploadFile, File, BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request

from scraper_utils import extract_sirens_from_csv, scrape_sirens, FIELDNAMES


templates = Jinja2Templates(directory="templates")

app = FastAPI()

# In-memory job store
jobs: Dict[str, Dict[str, Any]] = {}


def extract_sirens_from_csv_file(path: Path) -> Set[str]:
    """Extract SIREN numbers from a CSV file path."""
    with path.open("r", encoding="utf-8", newline="") as f:
        return extract_sirens_from_csv(f)


def scrape_and_save(job_id: str, csv_path: Path, sleep: float = 0.5, threads: int = 4) -> None:
    try:
        sirens = extract_sirens_from_csv_file(csv_path)
        results = scrape_sirens(sirens, sleep, threads)
        output_path = csv_path.with_suffix(".out.csv")
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            for row in results:
                writer.writerow(row)
        jobs[job_id]["status"] = "completed"
        jobs[job_id]["result"] = str(output_path)
    except Exception as e:
        jobs[job_id]["status"] = f"error: {e}"
    finally:
        if csv_path.exists():
            csv_path.unlink()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "jobs": jobs})


@app.post("/upload")
async def upload(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    job_id = str(uuid.uuid4())
    temp_path = Path(f"uploads/{job_id}.csv")
    temp_path.parent.mkdir(exist_ok=True)
    with temp_path.open("wb") as f:
        content = await file.read()
        f.write(content)
    jobs[job_id] = {"status": "processing"}
    background_tasks.add_task(scrape_and_save, job_id, temp_path)
    return {"job_id": job_id}


@app.get("/status/{job_id}")
async def status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return {"error": "job not found"}
    return {"job_id": job_id, "status": job.get("status")}


@app.get("/download/{job_id}")
async def download(job_id: str):
    job = jobs.get(job_id)
    if not job or job.get("status") != "completed":
        return {"error": "job not finished"}
    return FileResponse(job["result"], media_type="text/csv", filename="results.csv")
