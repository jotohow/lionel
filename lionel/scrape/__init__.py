import os
import sys
from pathlib import Path

# Add root to path when module is run as a script
ROOT = Path(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(str(ROOT)))
