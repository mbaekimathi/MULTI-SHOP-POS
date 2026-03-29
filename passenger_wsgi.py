# Phusion Passenger / cPanel Python app entry — loads Flask and runs startup schema sync in app.py.
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import app as application
