import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from marshmallow import fields
if not hasattr(fields.Number, "num_type"):
    fields.Number.num_type = float