"""
Test configuration to help with imports and test discovery.
This file is automatically loaded by test runners.
"""

import sys
import os

# Add parent directory to Python path so queuepipeio can be imported
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Add tests directory to path for local imports
tests_dir = os.path.dirname(os.path.abspath(__file__))
if tests_dir not in sys.path:
    sys.path.insert(0, tests_dir)