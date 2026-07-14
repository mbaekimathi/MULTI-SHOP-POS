"""Load environment: tracked ``.env.example`` first, then optional ``.env`` (overrides).

After env files load, auto-detect cPanel/Passenger hosting and fill only missing keys
so production ``.env`` can stay minimal (password + secret).
"""

from pathlib import Path

from dotenv import dotenv_values, load_dotenv

_ROOT = Path(__file__).resolve().parent
_ENV_PATH = _ROOT / ".env"
_EXAMPLE_PATH = _ROOT / ".env.example"

# Example file = committed defaults. Local ``.env`` overrides any same-named keys.
load_dotenv(_EXAMPLE_PATH, override=False)
load_dotenv(_ENV_PATH, override=True)

_env_file_vals = dotenv_values(_ENV_PATH) if _ENV_PATH.is_file() else {}
_env_file_keys = {k for k, v in (_env_file_vals or {}).items() if (v or "").strip()}

# RICHCOM_HOSTED in .env.example must not block auto-detect when .env omits it.
if "RICHCOM_HOSTED" not in _env_file_keys:
    import os

    os.environ.pop("RICHCOM_HOSTED", None)

try:
    from hosting_detect import apply_hosted_env_defaults

    apply_hosted_env_defaults(env_file_keys=_env_file_keys)
except Exception:
    pass
