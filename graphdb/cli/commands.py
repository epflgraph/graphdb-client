# graphdb/cli/commands.py
# This module defines the handler functions for each CLI command.
import json, rich
from pathlib import Path
from yaml import safe_load
from graphdb.models.sqlquery import SQLQuery

#-------------------------------------#
# Handler: Print index configuration  #
#-------------------------------------#
def cmd_config(args):
    """
    Usage:
        graphdb config print [...]
    """

    # Print headers
    print("🖥️  ~ GraphDB client CLI. Print out config options.")

    config_path = Path(__file__).resolve().parents[2] / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = safe_load(f) or {}

    def _redact(data):
        if isinstance(data, dict):
            return {
                key: ("***REDACTED***" if "password" in key.lower() else _redact(value))
                for key, value in data.items()
            }
        if isinstance(data, list):
            return [_redact(item) for item in data]
        return data

    rich.print_json(data=_redact(config))

    # Print footers
    print("🖥️  ~ Done.")

#-----------------------------------#
# Handler: Test server connectivity #
#-----------------------------------#
def cmd_test(args):
    """
    Usage:
        graphdb test [...]
    """

    # Fetch context objects
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ GraphDB client CLI. Test server connectivity.")

    # Execute command:
    # - Test connection to MySQL server
    if args.env:
        if db.test(engine_name=args.env) is True:
            print(f"✅ MySQL server is up and running [env='{args.env}'].")
        else:
            print(f"❌ MySQL server is down or unreachable [env='{args.env}'].")
    else:
        for engine in db.engine.keys():
            if db.test(engine_name=engine) is True:
                print(f"✅ MySQL server is up and running [env='{engine}'].")
            else:
                print(f"❌ MySQL server is down or unreachable [env='{engine}'].")

    # Print footers
    print("🖥️  ~ Done.")

#-------------------------------------#
# Handler: Showcase SQLQuery features #
#-------------------------------------#
def cmd_inspect(args):
    """
    Usage:
        graphdb execute [...]
    """

    print("🖥️  ~ GraphDB client CLI. SQLQuery feature showcase.")

    # Build query either from raw SQL or from SELECT/FROM(/WHERE) parts.
    if args.query:
        query_obj = SQLQuery(
            query=args.query,
            description=args.description,
            db=args.env,
            title=args.title,
            elapsed_ms=args.elapsed_ms,
            row_count=args.row_count,
            error=args.error,
            box_style=args.box_style,
            theme=args.theme,
            copyable=args.copyable,
            redact_params=not args.no_redact_params,
        )
    else:
        if not args.select or not args.from_:
            raise ValueError("Provide either --query OR both --select and --from.")
        query_obj = SQLQuery.from_parts(
            select=args.select,
            from_=args.from_,
            where=args.where,
            title=args.title,
            description=args.description,
            db=args.env,
            elapsed_ms=args.elapsed_ms,
            row_count=args.row_count,
            error=args.error,
            box_style=args.box_style,
            theme=args.theme,
            copyable=args.copyable,
            redact_params=not args.no_redact_params,
        )

    # Parse params from JSON when provided.
    if args.params_json:
        try:
            query_obj.params = json.loads(args.params_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid --params-json payload: {exc}") from exc

    # Optional textual diagnostics.
    if args.show_canonical:
        print("\nCanonical SQL:")
        print(query_obj.canonical_sql())

    if args.show_one_line:
        print("\nOne-line SQL:")
        print(query_obj.one_line_sql(max_len=args.one_line_len))

    if args.show_fingerprint:
        print("\nFingerprint:")
        print(query_obj.fingerprint(include_params=args.fingerprint_with_params))

    if args.snapshot:
        print("\nDebug snapshot:")
        print(json.dumps(query_obj.debug_snapshot(), indent=2, default=str))

    # Optional timing demos.
    if args.time_demo:
        out = query_obj.execute_with_timing(lambda: [1, 2, 3])
        print(f"\nTiming demo success: rows={len(out)} elapsed_ms={query_obj.elapsed_ms:.3f}")

    if args.time_fail_demo:
        try:
            query_obj.execute_with_timing(lambda: (_ for _ in ()).throw(RuntimeError("demo failure")))
        except RuntimeError as exc:
            print(f"\nTiming demo failure captured: {exc}")

    # Render output as normal or debug.
    if args.debug:
        query_obj.print_debug()
    else:
        query_obj.print()

    print("🖥️  ~ Done.")

#--------------------------------------------#
# Handler: Export database into local folder #
#--------------------------------------------#
def cmd_export(args):
    """
    Usage:
        graphdb export [...]
    """

    # Fetch context objects
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ GraphDB client CLI. Export database into local folder.")

    # Get export options
    t = args.table_name is not None
    c = args.include_create_tables
    d = args.include_data

    #------------------#
    # Execute commands #
    #------------------#

    # Export MySQL table definitions only
    if t and c and not d:
        db.export_create_table(
            engine_name   = args.env,
            schema_name   = args.schema_name,
            table_name    = args.table_name,
            output_folder = args.output_folder
        )

    # Export MySQL table definitions and data
    elif t and d:
        db.export_table(
            engine_name   = args.env,
            schema_name   = args.schema_name,
            table_name    = args.table_name,
            output_folder = args.output_folder,
            filter_by     = args.filter_by,
            chunk_size    = args.chunk_size,
            include_create_tables = c
        )

    # Export MySQL table definitions for entire database
    elif not t and c and not d:
        db.export_create_tables_in_database(
            engine_name   = args.env,
            schema_name   = args.schema_name,
            output_folder = args.output_folder
        )

    # Export MySQL table definitions and data for entire database
    elif not t and d:
        db.export_database(
            engine_name   = args.env,
            schema_name   = args.schema_name,
            output_folder = args.output_folder,
            filter_by     = args.filter_by,
            chunk_size    = args.chunk_size,
            include_create_tables = c
        )

    # Nothing to do
    else:
        print("⚠️  No export action specified. Please provide valid options.")

    # Print footers
    print("🖥️  ~ Done.")

#--------------------------------------------#
# Handler: Import database from local folder #
#--------------------------------------------#
def cmd_import(args):
    """
    Usage:
        graphdb import [...]
    """

    # Fetch context objects
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ GraphDB client CLI. Import database from local folder.")

    # Get import options
    t = args.table_name is not None
    c = args.include_create_tables
    d = args.include_data
    i = args.ignore_existing

    #------------------#
    # Execute commands #
    #------------------#

    # Import MySQL table definitions only
    if t and c and not d:
        db.import_create_table(
            engine_name  = args.env,
            schema_name  = args.schema_name,
            input_folder = f"{args.input_folder}/{args.table_name}",
            include_keys = False,
            ignore_existing = i
        )

    # Import MySQL table definitions and data
    elif t and d:
        db.import_table(
            engine_name  = args.env,
            schema_name  = args.schema_name,
            input_folder = f"{args.input_folder}/{args.table_name}",
            create_keys_after_import = True,
            ignore_existing = i
        )

    # Import MySQL table definitions and data for entire database
    elif not t:
        db.import_database(
            engine_name  = args.env,
            schema_name  = args.schema_name,
            input_folder = args.input_folder,
            create_keys_after_import = True,
            ignore_existing = i
        )

    # Nothing to do
    else:
        print("⚠️  No import action specified. Please provide valid options.")

    # Print footers
    print("🖥️  ~ Done.")

#-------------------------------------------------#
# Handler: Copy database or tables across servers #
#-------------------------------------------------#
def cmd_copy(args):
    """
    Usage:
        graphdb copy [...]
    """

    # Fetch context objects
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ GraphDB client CLI. Copy database or tables across servers.")

    # Get import options
    t = args.table_name is not None

    #------------------#
    # Execute commands #
    #------------------#

    # Import MySQL table definitions only
    if t:
        db.copy_table(
            source_engine_name   = args.from_env,
            source_schema_name   = args.from_schema,
            target_engine_name   = args.to_env,
            target_schema_name   = args.to_schema,
            table_name           = args.table_name,
            chunk_size           = args.chunk_size,
            create_keys_after_import = True
        )
    else:
        db.copy_database(
            source_engine_name   = args.from_env,
            source_schema_name   = args.from_schema,
            target_engine_name   = args.to_env,
            target_schema_name   = args.to_schema,
            chunk_size           = args.chunk_size,
            create_keys_after_import = True
        )

    # Print footers
    print("🖥️  ~ Done.")

#----------------------------------------------------#
# Handler: Compare database or tables across servers #
#----------------------------------------------------#
def cmd_compare(args):
    """
    Usage:
        graphdb compare [...]
    """

    # Fetch context objects
    db = args.ctx.db

    # Print headers
    print("🖥️  ~ GraphDB client CLI. Compare database or tables across servers.")

    # Get import options
    t = args.table_name is not None
    e = args.exact_row_count

    #------------------#
    # Execute commands #
    #------------------#

    # Import MySQL table definitions only
    if t:
        db.compare_tables(
            source_engine_name   = args.from_env,
            source_schema_name   = args.from_schema,
            target_engine_name   = args.to_env,
            target_schema_name   = args.to_schema,
            table_name           = args.table_name,
            exact_row_count      = e
        )
    else:
        db.compare_databases(
            source_engine_name   = args.from_env,
            source_schema_name   = args.from_schema,
            target_engine_name   = args.to_env,
            target_schema_name   = args.to_schema,
            exact_row_count      = e
        )

    # Print footers
    print("🖥️  ~ Done.")
