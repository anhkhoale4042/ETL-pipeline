# cli/ingest_cli.py
import argparse
import json
import shutil
from pathlib import Path
from datetime import datetime
from src.etl.ingest import ingest
from src.etl.transform import transform
from src.etl.utils import write_jsonl

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

def run_messages(messages, raw_out_path: Path, processed_base: Path) -> None:
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
            print(json.dumps({"level": "error", "msg": "ingest_error", "error": str(e), "sample": (msg or "")[:200]}))
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
            print(json.dumps({"level": "error", "msg": "write_processed_failed", "error": str(e), "session_id": rec.get("session_id")}))
    print(json.dumps({"level": "info", "msg": "completed_run", "count_raw": len(raw_out), "count_processed": len(processed_out), "errors": errors}))

def gen_dummy(n=50):
    typos = ["teh", "recieve", "diabtes", "hipertension"]
    roles = ["user", "bot", "admin"]
    channels = ["web", "mobile", "ivr"]
    short_misc = [
        "  PaTient John Karlson   has   {typo}   contact: test{idx}@example.com   ",
        "Call me at (555) 010{d} or email test{idx}@example.com   ",
        "My SSN is 123-45-6789 and ID AB12{idx:03d}",
        "Follow-up note for diabtes treatment. phone: 555-010{d}",
    ]
    medical_cases = [
        "I am experiencing painful, burning urination and a cloudy white discharge from the urethral opening. What could I be suffering from?",
        "I have itching and a burning sensation on the glans of my penis. What could this be?",
    ]
    msgs = []
    for i in range(n):
        idx = i
        d = i % 10
        if i % 6 == 0:
            typo = typos[i % len(typos)]
            msg = short_misc[0].format(typo=typo, idx=idx)
        elif i % 6 == 1:
            msg = short_misc[1].format(d=d, idx=idx)
        elif i % 6 == 2:
            msg = short_misc[2].format(idx=idx)
        elif i % 6 == 3:
            msg = short_misc[3].format(d=d)
        elif i % 6 == 4:
            msg = medical_cases[(i // 6) % len(medical_cases)]
        else:
            msg = medical_cases[(i // 6 + 1) % len(medical_cases)]
        meta = {
            "user_id": f"user{i%5}",
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "user_role": roles[i % len(roles)],
            "channel": channels[i % len(channels)],
        }
        msgs.append((msg, meta))
    return msgs

def main():
    parser = argparse.ArgumentParser(description="Ingest CLI - produce raw + processed JSONL")
    parser.add_argument("--dummy", action="store_true")
    parser.add_argument("--n", type=int, default=50)
    parser.add_argument("--output-raw", type=str, default=str(DEFAULT_RAW))
    parser.add_argument("--processed-dir", type=str, default=str(DEFAULT_PROCESSED))
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    raw_path = Path(args.output_raw)
    processed_base = Path(args.processed_dir)
    if args.overwrite:
        _safe_remove_path(raw_path)
        _safe_remove_path(processed_base)
    if args.dummy:
        messages = gen_dummy(args.n)
    else:
        import sys
        messages = []
        for line in sys.stdin:
            try:
                j = json.loads(line)
                messages.append((j.get("message"), j.get("metadata", {})))
            except Exception:
                continue
    run_messages(messages, raw_path, processed_base)

if __name__ == "__main__":
    main()
