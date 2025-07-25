import os
from typing import Any, List, Optional
from supabase import create_client, Client


def get_client() -> Optional[Client]:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    if not url or not key:
        return None
    return create_client(url, key)


def insert_records(records: List[dict[str, Any]]) -> None:
    client = get_client()
    if not client:
        return
    for rec in records:
        client.table("companies").insert(rec).execute()
