# src/etl/redact.py
from __future__ import annotations
import re
from typing import Tuple, List, Dict
from pathlib import Path
import os

WORDLIST_PATH = Path("data/resources/wordlist.txt")
STOPWORDS_PATH = Path("data/resources/stopwords.txt")

def _load_wordset(p: Path) -> set:
    s = set()
    if not p.exists():
        return s
    try:
        with p.open(encoding="utf-8") as fh:
            for line in fh:
                t = line.strip()
                if not t or t.startswith("#"):
                    continue
                s.add(t.lower())
    except Exception:
        return set()
    return s

MEDICAL_WHITELIST = _load_wordset(WORDLIST_PATH)
STOPWORDS = _load_wordset(STOPWORDS_PATH)

NER_ENABLED = False
_spacy_nlp = None
_spacy_model = os.getenv("SPACY_MODEL", "en_core_web_sm")
try:
    import spacy
    try:
        _spacy_nlp = spacy.load(_spacy_model)
        NER_ENABLED = True
    except Exception:
        try:
            spacy.cli.download(_spacy_model)
            _spacy_nlp = spacy.load(_spacy_model)
            NER_ENABLED = True
        except Exception:
            NER_ENABLED = False
except Exception:
    NER_ENABLED = False

EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
PHONE_RE = re.compile(r"\b(?=(?:.*\d){7,})[+\d\-\.\s()]{7,20}\b")
IP_V4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
MRN_RE = re.compile(r"\b(?:MRN|mrn|MedicalRecord|medicalrecord|MRN[-\s]?)\d+\b", re.IGNORECASE)
GENERIC_ID_RE = re.compile(r"(?<![@.])\b(?=.*\d)(?=.*[A-Za-z])[A-Za-z0-9\-]{6,}\b(?!@)", re.IGNORECASE)
URL_RE = re.compile(r"https?:\/\/[^\s]+", re.IGNORECASE)
DATE_RE = re.compile(r"\b(?:\d{4}-\d{2}-\d{2}|\d{1,2}[\/.\-]\d{1,2}[\/.\-]\d{2,4}|\d{1,2}\s(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec))\b", re.IGNORECASE)

_PATTERNS = [
    ("EMAIL", EMAIL_RE),
    ("SSN", SSN_RE),
    ("PHONE", PHONE_RE),
    ("IP", IP_V4_RE),
    ("URL", URL_RE),
    ("MEDICAL_RECORD", MRN_RE),
    ("ID", GENERIC_ID_RE),
    ("DATE", DATE_RE),
]

_PLACEHOLDER = {
    "EMAIL": "[REDACTED_EMAIL]",
    "PHONE": "[REDACTED_PHONE]",
    "SSN": "[REDACTED_SSN]",
    "IP": "[REDACTED_IP]",
    "URL": "[REDACTED_URL]",
    "MEDICAL_RECORD": "[REDACTED_ID]",
    "ID": "[REDACTED_ID]",
    "DATE": "[REDACTED_DATE]",
    "NAME": "[REDACTED_NAME]",
    "GPE": "[REDACTED_LOCATION]",
    "ORG": "[REDACTED_ORG]",
}

_TYPE_PRIORITY = ["EMAIL", "SSN", "PHONE", "MEDICAL_RECORD", "ID", "DATE", "IP", "URL", "NAME", "GPE", "ORG"]

def _type_preference(types):
    if not types:
        return None
    if not isinstance(types, list):
        types = [types]
    for t in _TYPE_PRIORITY:
        if t in types:
            return t
    return types[0] if types else None

def _should_skip_phi_tag(typ: str, match_text: str, context_text: str = "") -> bool:
    lower = match_text.lower().strip(" .,:;\"'()[]{}")
    if not lower:
        return True
    if typ in ("EMAIL", "PHONE", "SSN", "URL", "IP"):
        return False
    if typ == "DATE":
        ctx = (context_text or "").lower()
        lower = match_text.lower().strip()
        if re.match(r"^\d+\s*(day|days|week|weeks|month|months|year|years)\b", lower):
            return True
        if lower in ("yesterday", "today", "tomorrow", "last week", "next month"):
            return True
        if re.search(r"\b(appointment|schedule|book|booking|visit|meeting|checkup|follow[- ]?up)\b", ctx):
            return True
        if re.search(r"\b(birth|dob|born|admit|admitted|discharge|hospital|surgery|diagnos)\b", ctx):
            return False
        return False
    if lower in MEDICAL_WHITELIST or lower in STOPWORDS:
        return True
    if typ in ("ID", "MEDICAL_RECORD"):
        core = re.sub(r"\W", "", match_text)
        if re.search(r"\d", match_text) and re.search(r"[A-Za-z]", match_text) and len(core) >= 6:
            return False
        return True
    return False

def _detect_regex_spans(text: str) -> List[Dict]:
    spans = []
    for typ, patt in _PATTERNS:
        for m in patt.finditer(text):
            match_text = m.group(0)
            if typ == "PHONE":
                digits = re.sub(r"\D", "", match_text)
                if len(digits) < 7:
                    continue
            start = max(0, m.start() - 40)
            end = min(len(text), m.end() + 40)
            context_window = text[start:end]
            if _should_skip_phi_tag(typ, match_text, context_text=context_window):
                continue
            spans.append({"type": typ, "start": m.start(), "end": m.end(), "match": match_text})
    spans.sort(key=lambda s: (s["start"], -(s["end"] - s["start"])))
    return _merge_overlapping_spans(spans)

def _detect_ner_spans(text: str) -> List[Dict]:
    if not NER_ENABLED or not _spacy_nlp:
        return []
    doc = _spacy_nlp(text)
    spans = []
    for ent in doc.ents:
        label = ent.label_
        typ = None
        if label == "PERSON":
            typ = "NAME"
        elif label in ("GPE", "LOC", "FAC"):
            typ = "GPE"
        elif label == "DATE":
            typ = "DATE"
        elif label == "ORG":
            typ = "ORG"
        else:
            continue
        match_text = ent.text
        lower = match_text.lower().strip(" .,:;\"'()[]{}")
        if lower in MEDICAL_WHITELIST or lower in STOPWORDS:
            continue
        if typ == "DATE":
            lower = match_text.lower().strip()
            if re.match(r"^(\d+|a|one|two|three|four|five|six|seven|eight|nine|ten)\s+(day|days|week|weeks|month|months|year|years)\b", lower):
                continue
            digits = re.sub(r"\D", "", match_text)
            if digits and len(digits) < 5 and not re.search(r"[A-Za-z]", match_text):
                continue
            if digits and len(digits) >= 7 and not re.search(r"[A-Za-z]", match_text):
                typ = "PHONE"
        start = max(0, ent.start_char - 40)
        end = min(len(text), ent.end_char + 40)
        context_window = text[start:end]
        if _should_skip_phi_tag(typ, match_text, context_text=context_window):
            continue
        spans.append({"type": typ, "start": ent.start_char, "end": ent.end_char, "match": match_text})
    spans.sort(key=lambda s: (s["start"], -(s["end"] - s["start"])))
    return _merge_overlapping_spans(spans)

def _merge_overlapping_spans(spans: List[Dict]) -> List[Dict]:
    if not spans:
        return []
    merged = []
    for s in spans:
        if not merged:
            merged.append(s.copy())
            continue
        last = merged[-1]
        if s["start"] <= last["end"]:
            last["end"] = max(last["end"], s["end"])
            a = last.get("type")
            b = s.get("type")
            if isinstance(a, list):
                if isinstance(b, list):
                    for bt in b:
                        if bt not in a:
                            a.append(bt)
                else:
                    if b not in a:
                        a.append(b)
                last["type"] = a
            else:
                if a == b:
                    last["type"] = a
                else:
                    types = [a] if a else []
                    if isinstance(b, list):
                        for bt in b:
                            if bt not in types:
                                types.append(bt)
                    else:
                        if b not in types:
                            types.append(b)
                    last["type"] = types
        else:
            merged.append(s.copy())
    return merged

def detect_phi_spans(text: str) -> List[Dict]:
    if not text:
        return []
    regex_spans = _detect_regex_spans(text)
    ner_spans = _detect_ner_spans(text)
    all_spans = sorted(regex_spans + ner_spans, key=lambda s: (s["start"], -(s["end"] - s["start"])))
    return _merge_overlapping_spans(all_spans)

def redact_text(text: str) -> Tuple[List[str], str]:
    if not text:
        return [], ""
    spans = detect_phi_spans(text)
    if not spans:
        return [], text
    out_parts = []
    last = 0
    flags = set()
    for s in spans:
        out_parts.append(text[last:s["start"]])
        typ_field = s["type"]
        chosen = _type_preference(typ_field)
        placeholder = _PLACEHOLDER.get(chosen, "[REDACTED]")
        prefix = out_parts[-1]
        if prefix and not prefix[-1].isspace() and prefix[-1].isalnum():
            out_parts.append(" ")
        out_parts.append(placeholder)
        next_char = text[s["end"]] if s["end"] < len(text) else ""
        if next_char and not next_char.isspace() and next_char.isalnum():
            out_parts.append(" ")
        if chosen:
            flags.add(chosen)
        last = s["end"]
    out_parts.append(text[last:])
    out_text = "".join(out_parts)
    out_text = re.sub(r"\s+", " ", out_text).strip()
    return sorted(flags), out_text

def redact_entities(entities: List[Dict]):
    if not entities:
        return [], []
    added = set()
    out = []
    for e in entities:
        v = e.get("value")
        if not v:
            out.append(e)
            continue
        flags, redacted = redact_text(str(v))
        if flags:
            added.update(flags)
            e = {**e, "value": redacted}
        out.append(e)
    return out, sorted(added)
