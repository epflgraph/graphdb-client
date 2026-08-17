# graphdb/cli/commands/export.py


def cmd_export(args):
    print("🖥️  ~ GraphDB client CLI. Export database into local folder.")

    service = args.ctx.container.export_ops
    t = args.table_name is not None
    c = args.include_create_tables
    d = args.include_data
    z = args.compress

    if t and c and not d:
        service.export_create_table(args.env, args.schema_name, args.table_name, args.output_folder)
    elif t and d:
        service.export_table(
            args.env,
            args.schema_name,
            args.table_name,
            args.output_folder,
            filter_by=args.filter_by,
            chunk_size=args.chunk_size,
            include_create_tables=c,
            compress=z,
        )
    elif not t and c and not d:
        service.export_create_tables_in_database(args.env, args.schema_name, args.output_folder)
    elif not t and d:
        service.export_database(
            args.env,
            args.schema_name,
            args.output_folder,
            filter_by=args.filter_by,
            chunk_size=args.chunk_size,
            include_create_tables=c,
            compress=z,
        )
    else:
        print("⚠️  No export action specified. Please provide valid options.")

    print("🖥️  ~ Done.")
