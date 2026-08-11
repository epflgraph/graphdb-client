# graphdb/cli/commands/copy.py


def cmd_copy(args):
    print("🖥️  ~ GraphDB client CLI. Copy database or tables across servers.")

    service = args.ctx.container.copy_ops

    if args.table_name:
        service.copy_table(
            args.from_env,
            args.from_schema,
            args.to_env,
            args.to_schema,
            args.table_name,
            chunk_size=args.chunk_size,
            create_keys_after_import=True,
            compress=args.compress,
        )
    else:
        service.copy_database(
            args.from_env,
            args.from_schema,
            args.to_env,
            args.to_schema,
            chunk_size=args.chunk_size,
            create_keys_after_import=True,
            compress=args.compress,
        )

    print("🖥️  ~ Done.")
