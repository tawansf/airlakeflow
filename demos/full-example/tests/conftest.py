import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
dags_path = root / "dags"
if str(dags_path) not in sys.path:
    sys.path.insert(0, str(dags_path))
