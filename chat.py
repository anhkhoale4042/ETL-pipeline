from __future__ import annotations
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict, constr, confloat

UUIDPattern = constr(
    pattern=r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
ISO8601_UTC = constr(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
LangCode = constr(pattern=r"^[a-z]{2}$")
ShortStr = constr(max_length=256)
TextStr = constr(max_length=4000)
IntentStr = constr(max_length=100)
PHIEnum = constr(pattern=r"^(NAME|PHONE|EMAIL|DATE|ID|IP|ADDRESS|SSN|MEDICAL_RECORD|OTHER)$")

class AuditEvent(BaseModel):
    event_id: UUIDPattern = Field(...)
    actor: ShortStr = Field(...)
    action: constr(pattern=r"^(ingest|redact|access|delete|export|annotate)$") = Field(...)
    timestamp: ISO8601_UTC = Field(...)
    reason: Optional[constr(max_length=1000)] = None
    details: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="forbid")

class Entity(BaseModel):
    type: constr(max_length=50)
    value: constr(max_length=1000)
    confidence: Optional[confloat(ge=0.0, le=1.0)] = None

    model_config = ConfigDict(extra="forbid")

class ChatMessage(BaseModel):
    session_id: UUIDPattern = Field(...)
    message_id: UUIDPattern = Field(...)
    timestamp: ISO8601_UTC = Field(...)
    user_role: constr(pattern=r"^(user|bot|admin|system)$") = Field(...)
    channel: constr(pattern=r"^(web|mobile|api|slack|teams)$") = Field(...)
    raw_text: TextStr = Field(...)
    clean_text: TextStr = Field(...)
    language: LangCode = Field(...)
    phi_flags: List[PHIEnum] = Field(..., min_length=0)
    audit_trail: List[AuditEvent] = Field(..., min_length=1)

    intent: Optional[IntentStr] = None
    entities: Optional[List[Entity]] = None
    urgency: Optional[constr(pattern=r"^(low|medium|high|critical)$")] = None
    confidence: Optional[confloat(ge=0.0, le=1.0)] = None
    consent_given: Optional[bool] = None
    retention_policy: Optional[constr(max_length=64)] = None

    model_config = ConfigDict(extra="forbid")

class ChatSession(BaseModel):
    session_id: UUIDPattern = Field(...)
    user_id: Optional[ShortStr] = None
    started_at: ISO8601_UTC = Field(...)
    ended_at: Optional[ISO8601_UTC] = None
    channel: Optional[constr(pattern=r"^(web|mobile|api|slack|teams)$")] = None
    messages: List[ChatMessage] = Field(..., min_length=1)
    metadata: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="forbid")
