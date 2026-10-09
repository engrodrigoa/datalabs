import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("LOG_FORMAT", "text")
os.environ.setdefault("OBS_PERSIST", "false")  # unit tests never write to the audit schema
