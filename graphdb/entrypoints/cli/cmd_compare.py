# graphdb/entrypoints/cli/cmd_compare.py
# Thin CLI handler: delegates to CompareOperations, which self-renders.


def cmd_compare(args):
    print("🖥️  ~ GraphDB client CLI. Compare database or tables across servers.")

    service = args.ctx.container.compare_ops

    if args.random_sampling:
        if args.table_name:
            service.compare_tables_by_random_sampling(
                args.from_env,
                args.from_schema,
                args.table_name,
                args.to_env,
                args.to_schema,
                args.table_name,
                sample_size=args.sample_size,
            )
        else:
            service.compare_all_tables_by_random_sampling(
                args.from_env,
                args.from_schema,
                args.to_env,
                args.to_schema,
                sample_size=args.sample_size,
            )
    elif args.table_name:
        service.compare_tables_by_metadata(
            args.from_env,
            args.from_schema,
            args.to_env,
            args.to_schema,
            args.table_name,
            row_count_tolerance=args.row_count_tolerance,
            ignore_warnings=args.ignore_warnings,
        )
    else:
        service.compare_databases_by_metadata(
            args.from_env,
            args.from_schema,
            args.to_env,
            args.to_schema,
            row_count_tolerance=args.row_count_tolerance,
            ignore_warnings=args.ignore_warnings,
        )

    print("🖥️  ~ Done.")
