# Storage Plan for Chatbot Prototype

## 1. Goal

## 2. Entities
- Session: Represents a conversation session.
- Message: Individual chat messages belonging to a session.
- AuditEvent: Records actions taken on messages (e.g., ingest, redact).
- KeyStore: Placeholder for encryption or key management if needed later.

## 3. Prototype Storage (JSONL)
- Storage format: JSON Lines (*.jsonl)
- Partitioning strategy: /data/processed/YYYY/MM/DD/
- Each session stored as its own JSONL file containing all messages.

Example path:
/data/processed/YYYY/MM/DD/{session_id}.jsonl  
e.g., /data/processed/2025/09/04/3fa885f4-5717-4562-b3fc-2c963f66afa6.jsonl

## 4. Relational Mapping (PostgreSQL)
Tables:
- sessions(session_id PK, user_id, started_at, ended_at, channel, metadata)
- messages(message_id PK, session_id FK, timestamp, user_role, channel, raw_text, clean_text, language, intent, urgency, confidence, consent_given, retention_policy)
- entities(entity_id PK, message_id FK, type, value, confidence)
- audit_events(event_id PK, message_id FK, actor, action, timestamp, reason, details)
- keystore(key_id PK, key_value, created_at)

Indexes:
- sessions(session_id)
- messages(session_id, timestamp)
- audit_events(message_id, timestamp)

## 5. Document Mapping (MongoDB)
- sessions collection structure mirrors relational model.
- Example JSON document is provided in examples/mongo_session_example.json.

Indexes:
- sessions._id
- messages.message_id
- audit_events.message_id

## 6. Partitioning & Retention
- Partition key: /data/processed/YYYY/MM/DD/
- Session-level JSONL files for easy retrieval.
- Retention: configurable; prototype assumes keep-all, later support archival.
