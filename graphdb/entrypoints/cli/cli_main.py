# graphdb/cli/main.py
# Main entry point for the GraphDB CLI application using Typer.
from __future__ import annotations

import argparse
from typing import Optional

import typer

from graphdb.domain.models.mdl_config import GraphDBConfig
from graphdb.entrypoints.cli.cli_context import CLIContext
from graphdb.entrypoints.cli.container import Container
from graphdb.entrypoints.cli import (
    cmd_config,
    cmd_compare,
    cmd_copy,
    cmd_export,
    cmd_import,
    cmd_inspect,
    cmd_test,
)

app = typer.Typer(
    name="graphdb",
    help="GraphDB command-line interface (CLI) for managing MySQL server actions.",
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)


class _LazyAppState:
    """Container for lazily-initialized CLI dependencies."""

    def __init__(self) -> None:
        self._container: Optional[Container] = None

    @property
    def container(self) -> Container:
        if self._container is None:
            self._container = Container(GraphDBConfig.from_default_file())
        return self._container


@app.callback()
def _main_callback(ctx: typer.Context) -> None:
    """Attach shared lazy state to the Typer context."""
    ctx.obj = _LazyAppState()


def _cli_context(ctx: typer.Context) -> CLIContext:
    return CLIContext(container=ctx.obj.container)


# -----------------------------------------------------------------------------
# Helper to load configured environments for defaults/validation metadata.
# -----------------------------------------------------------------------------
def _load_env_settings() -> tuple[list[str], str, str]:
    try:
        cfg = GraphDBConfig.from_default_file()
        envs = list(cfg.env_names())
        default_env = cfg.default_env
        second_env = envs[1] if len(envs) > 1 else default_env
        return envs, default_env, second_env
    except Exception:
        return [], "", ""


_ENVS, _DEFAULT_ENV, _SECOND_ENV = _load_env_settings()


# -----------------------------------------------------------------------------
# Command: config
# -----------------------------------------------------------------------------
config_app = typer.Typer(
    help="Manage and print configuration options.",
    context_settings={"help_option_names": ["-h", "--help"]},
)
app.add_typer(config_app, name="config")


@config_app.command("print", help="Print out config options.")
def _cmd_config_print(
    config: Optional[str] = typer.Option(None, "--config", help="Path to a specific config file (overrides GRAPHDB_CONFIG/.env)."),
) -> None:
    cmd_config(argparse.Namespace(ctx=None, config_path=config))


# -----------------------------------------------------------------------------
# Command: test
# -----------------------------------------------------------------------------
@app.command("test", help="Test server connectivity.")
def _cmd_test(
    ctx: typer.Context,
    env: str = typer.Option(_DEFAULT_ENV, "--env", help="Specify environment."),
) -> None:
    cmd_test(
        argparse.Namespace(
            ctx=_cli_context(ctx),
            env=env,
        )
    )


# -----------------------------------------------------------------------------
# Command: inspect
# -----------------------------------------------------------------------------
@app.command("inspect", help="Inspect and render SQLQuery capabilities from the CLI.")
def _cmd_inspect(
    ctx: typer.Context,
    env: str = typer.Option(_DEFAULT_ENV, "--env", help="Specify environment."),
    query: Optional[str] = typer.Option(None, "--query", help="Raw SQL query text."),
    select: Optional[str] = typer.Option(None, "--select", help="SELECT clause for SQLQuery.from_parts."),
    from_: Optional[str] = typer.Option(None, "--from", help="FROM clause for SQLQuery.from_parts."),
    where: Optional[str] = typer.Option(None, "--where", help="Optional WHERE clause for SQLQuery.from_parts."),
    description: str = typer.Option("", "--description", help="Human-readable SQL description."),
    title: str = typer.Option("SQL", "--title", help="Render title."),
    params_json: Optional[str] = typer.Option(None, "--params-json", help="JSON object/array of parameters."),
    elapsed_ms: Optional[float] = typer.Option(None, "--elapsed-ms", help="Preset elapsed time in ms."),
    row_count: Optional[int] = typer.Option(None, "--row-count", help="Preset row count metadata."),
    error: Optional[str] = typer.Option(None, "--error", help="Preset error metadata."),
    box_style: str = typer.Option("minimal", "--box-style", help="Rich panel box style."),
    theme: str = typer.Option("monokai", "--theme", help="Rich syntax theme."),
    copyable: bool = typer.Option(False, "--copyable", help="Render in copyable mode."),
    debug: bool = typer.Option(False, "--debug", help="Render debug panel with fingerprint and query id."),
    snapshot: bool = typer.Option(False, "--snapshot", help="Print debug snapshot dict."),
    show_fingerprint: bool = typer.Option(False, "--show-fingerprint", help="Print SQL fingerprint."),
    fingerprint_with_params: bool = typer.Option(False, "--fingerprint-with-params", help="Include params when generating fingerprint."),
    show_canonical: bool = typer.Option(False, "--show-canonical", help="Print canonical SQL (single-line normalized)."),
    show_one_line: bool = typer.Option(False, "--show-one-line", help="Print one-line SQL preview."),
    one_line_len: int = typer.Option(120, "--one-line-len", help="Max length for --show-one-line."),
    no_redact_params: bool = typer.Option(False, "--no-redact-params", help="Display params without redaction."),
    time_demo: bool = typer.Option(False, "--time-demo", help="Run execute_with_timing() with a successful fake executor."),
    time_fail_demo: bool = typer.Option(False, "--time-fail-demo", help="Run execute_with_timing() with a failing fake executor."),
) -> None:
    cmd_inspect(
        argparse.Namespace(
            ctx=None,
            env=env,
            query=query,
            select=select,
            from_=from_,
            where=where,
            description=description,
            title=title,
            params_json=params_json,
            elapsed_ms=elapsed_ms,
            row_count=row_count,
            error=error,
            box_style=box_style,
            theme=theme,
            copyable=copyable,
            debug=debug,
            snapshot=snapshot,
            show_fingerprint=show_fingerprint,
            fingerprint_with_params=fingerprint_with_params,
            show_canonical=show_canonical,
            show_one_line=show_one_line,
            one_line_len=one_line_len,
            no_redact_params=no_redact_params,
            time_demo=time_demo,
            time_fail_demo=time_fail_demo,
        )
    )


# -----------------------------------------------------------------------------
# Command: export
# -----------------------------------------------------------------------------
@app.command("export", help="Export database into local folder.")
def _cmd_export(
    ctx: typer.Context,
    env: str = typer.Option(_DEFAULT_ENV, "--env", help="Specify environment."),
    schema_name: str = typer.Option(..., "--schema_name", help="Name of the database/schema to export."),
    output_folder: str = typer.Option(..., "--output_folder", help="Output folder to save the exported data into."),
    table_name: Optional[str] = typer.Option(None, "--table_name", help="Name of the table to export (if exporting only one table)."),
    filter_by: str = typer.Option("TRUE", "--filter_by", help="Filter condition to apply to all tables."),
    chunk_size: int = typer.Option(10000, "--chunk_size", help="Number of documents to export per batch (default=10000)."),
    include_create_tables: bool = typer.Option(False, "--include_create_tables", "-c", help="Include table definitions in export."),
    include_data: bool = typer.Option(False, "--include_data", "-d", help="Include data in export."),
    compress: bool = typer.Option(False, "--compress", "-z", help="Compress exported SQL chunks with gzip as .sql.gz."),
) -> None:
    cmd_export(
        argparse.Namespace(
            ctx=_cli_context(ctx),
            env=env,
            schema_name=schema_name,
            output_folder=output_folder,
            table_name=table_name,
            filter_by=filter_by,
            chunk_size=chunk_size,
            include_create_tables=include_create_tables,
            include_data=include_data,
            compress=compress,
        )
    )


# -----------------------------------------------------------------------------
# Command: import
# -----------------------------------------------------------------------------
@app.command("import", help="Import database from local folder.")
def _cmd_import(
    ctx: typer.Context,
    env: str = typer.Option(_DEFAULT_ENV, "--env", help="Specify environment."),
    schema_name: str = typer.Option(..., "--schema_name", help="Name of the database/schema to import."),
    input_folder: str = typer.Option(..., "--input_folder", help="Input folder containing the data to import."),
    table_name: Optional[str] = typer.Option(None, "--table_name", help="Name of the table to import (if importing only one table)."),
    include_create_tables: bool = typer.Option(False, "--include_create_tables", "-c", help="Include table definitions in import."),
    include_data: bool = typer.Option(False, "--include_data", "-d", help="Include data in import."),
    ignore_existing: bool = typer.Option(False, "--ignore_existing", "-i", help="Soft ignore table creation and existing rows."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose mode."),
    compress: bool = typer.Option(False, "--compress", "-z", help="Import gzip-compressed .sql.gz data files."),
) -> None:
    cmd_import(
        argparse.Namespace(
            ctx=_cli_context(ctx),
            env=env,
            schema_name=schema_name,
            input_folder=input_folder,
            table_name=table_name,
            include_create_tables=include_create_tables,
            include_data=include_data,
            ignore_existing=ignore_existing,
            verbose=verbose,
            compress=compress,
        )
    )


# -----------------------------------------------------------------------------
# Command: copy
# -----------------------------------------------------------------------------
@app.command("copy", help="Copy database or tables across servers.")
def _cmd_copy(
    ctx: typer.Context,
    from_env: str = typer.Option(_DEFAULT_ENV, "--from_env", help="Source environment."),
    to_env: str = typer.Option(_SECOND_ENV, "--to_env", help="Target environment."),
    from_schema: str = typer.Option(..., "--from_schema", help="Name of the source database/schema to copy from."),
    to_schema: str = typer.Option(..., "--to_schema", help="Name of the target database/schema to copy to."),
    table_name: Optional[str] = typer.Option(None, "--table_name", help="Name of the table to export (optional)."),
    chunk_size: int = typer.Option(10000, "--chunk_size", help="Number of rows to copy per batch (default=10000)."),
    compress: bool = typer.Option(False, "--compress", "-z", help="Compress exported SQL chunks with gzip as .sql.gz to save disk space."),
) -> None:
    cmd_copy(
        argparse.Namespace(
            ctx=_cli_context(ctx),
            from_env=from_env,
            to_env=to_env,
            from_schema=from_schema,
            to_schema=to_schema,
            table_name=table_name,
            chunk_size=chunk_size,
            compress=compress,
        )
    )


# -----------------------------------------------------------------------------
# Command: compare
# -----------------------------------------------------------------------------
@app.command("compare", help="Compare database or tables across servers.")
def _cmd_compare(
    ctx: typer.Context,
    from_env: str = typer.Option(_DEFAULT_ENV, "--from_env", help="Source environment."),
    to_env: str = typer.Option(_SECOND_ENV, "--to_env", help="Target environment."),
    from_schema: str = typer.Option(..., "--from_schema", help="Name of the source database/schema to compare."),
    to_schema: str = typer.Option(..., "--to_schema", help="Name of the target database/schema to compare."),
    table_name: Optional[str] = typer.Option(None, "--table_name", help="Name of the table to compare (if comparing only one table)."),
    row_count_tolerance: float = typer.Option(0.10, "--row_count_tolerance", help="Relative row-count difference below which a mismatch is reported as a warning instead of an error (default: 0.10 for 10%)."),
    ignore_warnings: bool = typer.Option(False, "--ignore_warnings", "-iw", help="Skip output for tables that have only warnings and no errors."),
) -> None:
    cmd_compare(
        argparse.Namespace(
            ctx=_cli_context(ctx),
            from_env=from_env,
            to_env=to_env,
            from_schema=from_schema,
            to_schema=to_schema,
            table_name=table_name,
            row_count_tolerance=row_count_tolerance,
            ignore_warnings=ignore_warnings,
        )
    )


def main(argv: Optional[list[str]] = None) -> int:
    app(args=argv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
