"""Load environment: tracked ``.env.example`` first, then optional ``.env`` (overrides)."""

from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent

# Example file = committed defaults. Local ``.env`` overrides any same-named keys.
load_dotenv(_ROOT / ".env.example", override=False)
load_dotenv(_ROOT / ".env", override=True)
