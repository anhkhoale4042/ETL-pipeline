# src/etl/transform.py
from typing import Dict, Any, List
import re
from .redact import redact_entities

EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
PHONE_RE = re.compile(r"(\+?\d{1,3}[-.\s]?)?(\(?\d{2,4}\)?[-.\s]?)?\d{3,4}[-.\s]?\d{3,4}")
SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
GENERIC_ID_RE = re.compile(r"\b(?=.*\d)(?=.*[A-Za-z])[A-Za-z0-9\-]{6,}\b", re.IGNORECASE)

def detect_phi_basic(clean_text: str) -> List[str]:
    flags = set()
    if EMAIL_RE.search(clean_text):
        flags.add("EMAIL")
    if PHONE_RE.search(clean_text):
        flags.add("PHONE")
    if SSN_RE.search(clean_text):
        flags.add("SSN")
    if GENERIC_ID_RE.search(clean_text):
        flags.add("ID")
    return sorted(flags)

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

    phi_flags = record.get("phi_flags")
    if phi_flags is None:
        phi_flags = detect_phi_basic(out["clean_text"]) if out["clean_text"] else []
    out["phi_flags"] = sorted(phi_flags)

    if record.get("audit_trail"):
        out["audit_trail"] = record["audit_trail"]
    else:
        out["audit_trail"] = [{
            "event_id": record["message_id"],
            "actor": "ingestion-service",
            "action": "ingest",
            "timestamp": out["timestamp"],
        }]

    out["intent"] = record.get("intent") if "intent" in record else None

    out["entities"] = record.get("entities", []) or []
    if out["entities"]:
        new_entities, ent_flags = redact_entities(out["entities"])
        out["entities"] = new_entities
        out["phi_flags"] = sorted(set(out["phi_flags"] + ent_flags))

    out["urgency"] = record.get("urgency", None)
    out["confidence"] = record.get("confidence", None)
    out["consent_given"] = record.get("consent_given", False)

    out["retention_policy"] = record.get("retention_policy", "dev-30d")

    return out
