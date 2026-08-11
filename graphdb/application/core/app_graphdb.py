#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from sqlalchemy import create_engine as SQLEngine, text, event
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError
from sqlalchemy.dialects.mysql import dialect as MySQLDialect
from typing import Any, Dict, Optional
from loguru import logger as sysmsg
from tqdm import tqdm
from pathlib import Path
import numpy as np
import pandas as pd
from tabulate import tabulate
import sys, os, re, subprocess, json, datetime, hashlib, random, glob, time, rich, ssl, shlex, shutil, gzip, tempfile, types
from graphdb.application.core.cfg_config import GraphDBConfig, GraphDBConfigError
from graphdb.utils.mdl_sqlquery import print_sql
from graphdb.utils.cmn_table import get_table_type_from_name

# New architecture imports (incremental migration)
from graphdb.application.factories.fct_adapter_registry import AdapterRegistry
from graphdb.domain.mdl_connection import ConnectionParams
from graphdb.adapters.gateways.utils import (
    normalize_ssl_options,
    parse_bool,
    build_ssl_connect_args,
    detect_cli_option_names,
    build_ssl_cli_flags,
)
from graphdb.adapters.gateways.gtw_sqlalchemy import create_sqlalchemy_engine
from graphdb.adapters.engine.eng_execute import EngineExecuteAdapter
from graphdb.adapters.engine.eng_initiate import EngineInitiateAdapter
from graphdb.adapters.engine.eng_test import EngineTestAdapter
from graphdb.adapters.data.dta_compare import DataCompareAdapter
from graphdb.adapters.data.dta_copy import DataCopyAdapter
from graphdb.adapters.data.dta_export import DataExportAdapter
from graphdb.adapters.data.dta_import import DataImportAdapter
from graphdb.adapters.display.dis_print import DisplayAdapter, print_colour
from graphdb.adapters.data.dta_integrity import DataIntegrityAdapter
from graphdb.adapters.persistence.prs_cell import DataCellAdapter


# Find the repository root directory
REPO_ROOT = Path(__file__).resolve().parents[2]

#------------------------------------------------#
# Progress bar and system messages configuration #
#------------------------------------------------#

# Width of the progress bar
PBWIDTH = 64

# Set up system message handler to display TRACE messages
sysmsg.remove()
sysmsg.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
           "<level>{level: <8}</level> | "
           "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line:06d}</cyan> - "
           "<level>{message}</level>",
    level="TRACE"
)

#-----------------------------------------#
# Class definition for Graph MySQL engine #
#-----------------------------------------#
class GraphDB():

    # Class variable to hold the single instance
    _instance = None
    _cli_option_cache: Dict[str, set] = {}

    # Create new instance of class before __init__ is called
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)  # Use `object.__new__()` explicitly
            cls._instance._initialized = False  # Flag for initialization check
        return cls._instance

    # Class constructor
    def __init__(self, name="GraphDB", config: Optional[GraphDBConfig] = None):

        # Check if the instance is already initialized
        if not self._initialized:  # Prevent reinitialization
            self.name = name
            self._initialized = True

        self.config = config or GraphDBConfig.from_default_file()
        self.default_engine_name = self.config.default_env

        # Build new architecture adapters; mirror their state for backward compatibility.
        self._adapter_registry = AdapterRegistry(self.config)

        self.params = {}
        self.engine = {}
        self.base_command_mysql = {}
        self.base_command_mysqldump = {}
        self.subprocess_env = {}

        for env_name, env_config in self.config.environments.items():
            adapter = self._adapter_registry.get(env_name)
            self.params[env_name] = env_config.as_dict()
            self.engine[env_name] = adapter.engine
            self.base_command_mysql[env_name] = adapter.base_command_mysql
            self.base_command_mysqldump[env_name] = adapter.base_command_mysqldump
            self.subprocess_env[env_name] = adapter.subprocess_env

        # Adapter composition for migrated functionality
        self._execute_adapter = EngineExecuteAdapter(self)
        self._initiate_adapter = EngineInitiateAdapter(self)
        self._test_adapter = EngineTestAdapter(self)
        self._compare_adapter = DataCompareAdapter(self)
        self._copy_adapter = DataCopyAdapter(self)
        self._export_adapter = DataExportAdapter(self)
        self._import_adapter = DataImportAdapter(self)
        self._display_adapter = DisplayAdapter(self)
        self._integrity_adapter = DataIntegrityAdapter(self)
        self._cell_adapter = DataCellAdapter(self)


#================#
# Main execution #
#================#
if __name__ == "__main__":
    db = GraphDB()
    if db.test() is True:
        sysmsg.success("✅ MySQL client test passed.")
    else:
        sysmsg.error("❌ MySQL client test failed.")

