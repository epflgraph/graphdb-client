# graphdb/cli/commands/import_.py
from graphdb.application.import_service import ImportService


def cmd_import(args):
    print("🖥️  ~ GraphDB client CLI. Import database from local folder.")

    service = ImportService(args.ctx.registry)
    t = args.table_name is not None
    c = args.include_create_tables
    d = args.include_data
    i = args.ignore_existing
    v = args.verbose
    z = args.compress

    if t and c and not d:
        service.import_create_table(
            args.env,
            args.schema_name,
            f"{args.input_folder}/{args.table_name}",
            include_keys=False,
            ignore_existing=i,
            verbose=v,
        )
    elif t and d:
        service.import_table(
            args.env,
            args.schema_name,
            f"{args.input_folder}/{args.table_name}",
            create_keys_after_import=True,
            ignore_existing=i,
            verbose=v,
            compress=z,
        )
    elif not t:
        service.import_database(
            args.env,
            args.schema_name,
            args.input_folder,
            create_keys_after_import=True,
            ignore_existing=i,
            verbose=v,
            compress=z,
        )
    else:
        print("⚠️  No import action specified. Please provide valid options.")

    print("🖥️  ~ Done.")
