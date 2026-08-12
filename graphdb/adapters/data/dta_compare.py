from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from sqlalchemy import text
from tqdm import tqdm

from graphdb.adapters.environments import Environments
from graphdb.adapters.gateways.gtw_environment import EnvironmentGateway

class DataCompareAdapter:
    """Adapter for data operations."""

    def __init__(self, envs: Environments) -> None:
        self.envs = envs
