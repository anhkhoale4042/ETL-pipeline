# src/etl/ingest_cli.py
from pathlib import Path
import shutil
import json
from typing import List, Dict, Any, Tuple
from datetime import datetime
import random
from src.etl.transform import transform
from src.etl.ingest import ingest

DEFAULT_RAW = Path("data/raw/all_messages.jsonl")
DEFAULT_PROCESSED = Path("data/processed")


def partitioned_path(base: Path, ts_iso: str, session_id: str) -> Path:
    date_part = ts_iso.split("T")[0]
    y, m, d = date_part.split("-")
    return base / y / m / d / f"{session_id}.jsonl"


def _safe_remove_path(p: Path) -> None:
    try:
        if p.exists():
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                shutil.rmtree(p)
    except Exception as e:
        print(json.dumps({"level": "warning", "msg": "remove_failed", "path": str(p), "error": str(e)}))


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a"
    with path.open(mode, encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def run_messages(messages: List[Tuple[str, Dict[str, Any]]], raw_out_path: Path, processed_base: Path) -> None:
    raw_out = []
    processed_out = []
    errors = 0
    for msg, meta in messages:
        try:
            rec = ingest(msg, meta)
            raw_out.append(rec)
            out = transform(rec)
            processed_out.append(out)
        except Exception as e:
            errors += 1
            try:
                sample = {"msg": msg, "meta": meta}
            except Exception:
                sample = {"msg": "<unserializable>"}
            print(json.dumps({"level": "error", "msg": "ingest_error", "error": str(e), "sample": sample}))

    try:
        write_jsonl(raw_out_path, raw_out)
    except Exception as e:
        print(json.dumps({"level": "error", "msg": "write_raw_failed", "error": str(e), "path": str(raw_out_path)}))

    for rec in processed_out:
        try:
            p = partitioned_path(processed_base, rec["timestamp"], rec["session_id"])
            write_jsonl(p, [rec])
        except Exception as e:
            errors += 1
            sid = rec.get("session_id", "<unknown>")
            print(json.dumps({"level": "error", "msg": "write_processed_failed", "error": str(e), "session_id": sid}))

    print(json.dumps({"level": "info", "msg": "completed_run", "count_raw": len(raw_out), "count_processed": len(processed_out), "errors": errors}))


def gen_dummy(n: int = 50) -> List[Tuple[str, Dict[str, Any]]]:
    typos = ["teh", "recieve", "diabtes", "hipertension"]
    roles = ["user", "bot", "admin"]
    channels = ["web", "mobile", "ivr"]
    short_misc = [
        "PaTient John Karlson has {typo} contact: test{idx}@example.com",
        "Call me at (555) 010{d} or email test{idx}@example.com",
        "My SSN is 123-45-6789 and ID AB12{idx:03d}",
        "Follow-up note for diabetes treatment. phone: 555-010{d}",
    ]
    medical_cases = [
        "I am experiencing painful, burning urination and a cloudy white discharge from the urethral opening. What could this be?",
        "I have itching and a burning sensation on the glans of my penis. What could this be?",
    ]
    msgs: List[Tuple[str, Dict[str, Any]]] = []
    for i in range(n):
        idx = i
        d = i % 10
        mod6 = i % 6
        if mod6 == 0:
            typo = typos[i % len(typos)]
            msg = short_misc[0].format(typo=typo, idx=idx)
        elif mod6 == 1:
            msg = short_misc[1].format(d=d, idx=idx)
        elif mod6 == 2:
            msg = short_misc[2].format(idx=idx)
        elif mod6 == 3:
            msg = short_misc[3].format(d=d)
        elif mod6 == 4:
            msg = medical_cases[(i // 6) % len(medical_cases)]
        else:
            msg = medical_cases[((i // 6) + 1) % len(medical_cases)]
        meta = {
            "user_id": f"user{idx}",
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "user_role": roles[i % len(roles)],
            "channel": channels[i % len(channels)],
        }
        msgs.append((msg, meta))
    return msgs
