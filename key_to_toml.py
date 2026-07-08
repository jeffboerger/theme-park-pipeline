"""Converts a GCP service-account JSON key into a Streamlit-Cloud TOML block.

Usage:
    python key_to_toml.py keys/pipeline-key.json > secrets_TEMP.toml

Then open secrets_TEMP.toml in a text editor, copy ALL of it, paste into
Streamlit Cloud -> app -> Settings -> Secrets, save, and delete the temp file.
"""
import json
import sys

if len(sys.argv) != 2:
    print("Usage: python key_to_toml.py <path-to-key.json>")
    sys.exit(1)

with open(sys.argv[1]) as f:
    key = json.load(f)

print("[gcp_service_account]")
for field, value in key.items():
    if field == "private_key":
        # Use a TOML multi-line basic string (triple quotes) with REAL newlines.
        # This is the paste-safe form: the key body sits on its own lines
        # exactly as it appears in the JSON, no fragile \n escapes to mangle.
        real = value.replace("\\n", "\n")
        print(f'private_key = """\n{real}"""')
    else:
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        print(f'{field} = "{escaped}"')
