import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import json
import pytest
from jsonschema import validate, ValidationError as JsonSchemaValidationError
from pydantic import ValidationError as PydanticValidationError
from models.chat import ChatMessage  # relative to src

SCHEMA_FILE = "schemas/chat_message.schema.json"
SAMPLE_FILE = "examples/sample_records.jsonl"

# Load schema once
with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
    schema = json.load(f)

# Load all sample records (including any invalid ones)
with open(SAMPLE_FILE, "r", encoding="utf-8") as f:
    sample_records = [json.loads(line) for line in f if line.strip()]

@pytest.mark.parametrize("record", sample_records)
def test_record_validation_consistency(record):
    msg_id = record.get("message_id", "[no id]")

    # Pydantic validation
    try:
        ChatMessage(**record)
        pydantic_valid = True
        pydantic_err = None
    except PydanticValidationError as e:
        pydantic_valid = False
        pydantic_err = str(e).splitlines()[0]  

    # JSON Schema validation
    try:
        validate(instance=record, schema=schema)
        jsonschema_valid = True
        jsonschema_err = None
    except JsonSchemaValidationError as e:
        jsonschema_valid = False
        jsonschema_err = str(e).splitlines()[0]

    overall = "Valid" if (pydantic_valid and jsonschema_valid) else "Invalid"
    print(f"\nRecord {msg_id} -> {overall}")
    print(f"  Pydantic:  {'OK' if pydantic_valid else 'ERR'}"
          f"{'' if pydantic_valid else f' - {pydantic_err}'}")
    print(f"  JSONSchema: {'OK' if jsonschema_valid else 'ERR'}"
          f"{'' if jsonschema_valid else f' - {jsonschema_err}'}")

    assert pydantic_valid == jsonschema_valid, (
        f"Validator mismatch for record {msg_id}: "
        f"pydantic_valid={pydantic_valid}, jsonschema_valid={jsonschema_valid}"
    )
