# src/etl/utils.py
from pathlib import Path
import json
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

def make_uuid() -> str:
    return str(uuid.uuid4())

def to_iso_utc(ts: Optional[str]) -> str:
    if not ts:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        parsed_utc = parsed.astimezone(timezone.utc)
        return parsed_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        pass
    patterns = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S")
    for p in patterns:
        try:
            parsed = datetime.strptime(ts, p)
            parsed_utc = parsed.replace(tzinfo=timezone.utc)
            return parsed_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            continue
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
