# configs/base.py
"""
Base configuration classes and environment handling.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class EnvironmentVariables:
    """
    This is for the environmental variables:
    """

    env_file_path: Optional[str] = ".env"
