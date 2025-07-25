# Autocomplete Finder with Supabase

This project scrapes company data from Pappers and can store the results in a Supabase table. It exposes a FastAPI application that allows CSV uploads and background scraping.

## Configuration

Set the following environment variables to enable Supabase integration:

- `SUPABASE_URL` – your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY` (or `SUPABASE_KEY`) – API key with insert permissions

If these variables are not defined, data will only be written to the output CSV file.

## Running locally

```bash
pip install -r requirements.txt
uvicorn async_web_app:app --reload
```

Upload a CSV at `http://localhost:8000/`.

## Deploying to Fly.io

1. Install [flyctl](https://fly.io/docs/hands-on/install-flyctl/).
2. Run `fly launch` and follow the prompts (use the provided `Dockerfile`).
3. Set your Supabase credentials on Fly:
   ```bash
   fly secrets set SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=...
   ```
4. Deploy with `fly deploy`.

Once deployed, you can access your app via the Fly URL.
