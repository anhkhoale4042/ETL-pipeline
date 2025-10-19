# src/etl/transform.py
from typing import Dict, Any

def transform(record: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "session_id": record["session_id"],
        "message_id": record["message_id"],
        "timestamp": record["timestamp"],
        "user_role": record.get("user_role", "user"),
        "channel": record.get("channel", "web"),
        "raw_text": record.get("raw_text", "") or "",
        "clean_text": record.get("clean_text", "") or "",
        "language": record.get("language", "en") or "en",
    }

    out["phi_flags"] = sorted(record.get("phi_flags", []))

    out["audit_trail"] = record.get("audit_trail") or [{
        "event_id": record["message_id"],
        "actor": "ingestion-service",
        "action": "ingest",
        "timestamp": out["timestamp"],
    }]

    out["intent"] = record.get("intent")
    out["entities"] = record.get("entities", []) or []
    out["urgency"] = record.get("urgency")
    out["confidence"] = record.get("confidence")
    out["consent_given"] = record.get("consent_given", False)
    out["retention_policy"] = record.get("retention_policy", "dev-30d")

    return out
