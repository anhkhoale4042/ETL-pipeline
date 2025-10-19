#src/etl/ingest.py
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict
from datetime import datetime
from uuid import uuid4 as make_uuid

WORDLIST_PATH = Path("data/resources/wordlist.txt")
MAX_TEXT_LEN = 4000

_whitespace_re = re.compile(r"\s+")
_punct_only_re = re.compile(rf"^[{re.escape(re.escape(re.escape(re.escape('!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'))))}]+$")

try:
    from spellchecker import SpellChecker
    spell = SpellChecker()
except Exception:
    spell = None

MEDICAL_WHITELIST = set()
if WORDLIST_PATH.exists():
    try:
        with WORDLIST_PATH.open(encoding="utf-8") as fh:
            for line in fh:
                t = line.strip()
                if not t or t.startswith("#"):
                    continue
                MEDICAL_WHITELIST.add(t.lower())
    except Exception:
        MEDICAL_WHITELIST = set()

if MEDICAL_WHITELIST and spell is not None:
    try:
        spell.word_frequency.load_words(list(MEDICAL_WHITELIST))
    except Exception:
        for w in MEDICAL_WHITELIST:
            try:
                spell.word_frequency.add(w)
            except Exception:
                pass

SPECIAL_TOKENS = {"ssn", "id", "dob"}

def _is_token_numeric(token: str) -> bool:
    return any(ch.isdigit() for ch in token)

def _safe_spell_correction(token: str) -> str:
    if not token:
        return token
    if _punct_only_re.match(token):
        return token
    if _is_token_numeric(token):
        return token
    if token in MEDICAL_WHITELIST:
        return token
    if token in SPECIAL_TOKENS:
        return token
    if spell is None:
        return token
    corr = spell.correction(token)
    return corr if corr else token

def clean(raw_text: Any) -> str:
    if raw_text is None:
        return ""
    try:
        s = str(raw_text)
    except Exception:
        s = ""
    s = unicodedata.normalize("NFC", s)
    s = "".join(ch for ch in s if ch.isprintable())
    s = re.sub(r"(\s+([:;,.!?]))", r"\1", s)
    s = re.sub(r"([{[<])", r"\1 ", s)
    s = re.sub(r"([}\]>])", r" \1", s)
    s = _whitespace_re.sub(" ", s).strip()
    s = re.sub(r"(?<=\d),(?=\d{3}\b)", "", s)
    words = s.split()
    corrected_tokens = []
    for tok in words:
        if _punct_only_re.match(tok):
            corrected_tokens.append(tok)
            continue
        if _is_token_numeric(tok):
            corrected_tokens.append(tok)
            continue
        if tok in MEDICAL_WHITELIST:
            corrected_tokens.append(tok)
            continue
        if tok in SPECIAL_TOKENS:
            corrected_tokens.append(tok)
            continue
        corrected_tokens.append(_safe_spell_correction(tok))
    cleaned = " ".join(corrected_tokens)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    if len(cleaned) > MAX_TEXT_LEN:
        cleaned = cleaned[:MAX_TEXT_LEN]
    return cleaned

def ingest(message: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    rec = {}
    rec["session_id"] = metadata.get("session_id") or str(make_uuid())
    rec["message_id"] = metadata.get("message_id") or str(make_uuid())
    rec["timestamp"] = metadata.get("timestamp") or datetime.utcnow().isoformat()
    rec["user_role"] = metadata.get("user_role", "user")
    rec["channel"] = metadata.get("channel", "web")
    raw = message or ""
    rec["raw_text"] = raw
    redacted_raw = redact_text(raw)
    phi_flags, cleaned = [], clean(redacted_raw)
    ph_map = {
        "EMAIL": "[REDACTED_EMAIL]",
        "PHONE": "[REDACTED_PHONE]",
        "SSN": "[REDACTED_SSN]",
        "IP": "[REDACTED_IP]",
        "URL": "[REDACTED_URL]",
        "MEDICAL RECORD": "[REDACTED_ID]",
        "DATE": "[REDACTED_DATE]",
        "ID": "[REDACTED_ID]",
        "NAME": "[REDACTED_NAME]",
        "GPE": "[REDACTED_LOCATION]",
    }
    for ph in set(phi_flags):
        cleaned = cleaned.replace(ph_map.get(ph, "[REDACTED]").lower(), ph_map.get(ph, "[REDACTED]"))
    rec["clean_text"] = cleaned
    rec["phi_flags"] = sorted(phi_flags)
    lang = metadata.get("language", "en")
    entities = metadata.get("entities", []) or []
    redacted_entities, ent_flags = redact_entities(entities)
    rec["entities"] = redacted_entities
    if ent_flags:
        rec["phi_flags"] = sorted(set(rec.get("phi_flags", [])) | set(ent_flags))
    audits = []
    if rec["phi_flags"]:
        audits.append({"event_id": str(make_uuid()), "actor": "redaction-service", "action": "redact", "timestamp": datetime.utcnow().isoformat()})
    rec["audit_trail"] = audits
    rec["intent"] = metadata.get("intent")
    rec["urgency"] = metadata.get("urgency")
    rec["confidence"] = metadata.get("confidence")
    rec["consent_given"] = bool(metadata.get("consent_given", False))
    rec["retention_policy"] = metadata.get("retention_policy", "dev-30d")
    return rec
