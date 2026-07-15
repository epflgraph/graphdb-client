# graphdb/cli/commands/inspect.py
import json

from graphdb.models.sqlquery import SQLQuery


def cmd_inspect(args):
    print("🖥️  ~ GraphDB client CLI. SQLQuery feature showcase.")

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

    if args.params_json:
        try:
            query_obj.params = json.loads(args.params_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid --params-json payload: {exc}") from exc

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

    if args.time_demo:
        out = query_obj.execute_with_timing(lambda: [1, 2, 3])
        print(f"\nTiming demo success: rows={len(out)} elapsed_ms={query_obj.elapsed_ms:.3f}")

    if args.time_fail_demo:
        try:
            query_obj.execute_with_timing(lambda: (_ for _ in ()).throw(RuntimeError("demo failure")))
        except RuntimeError as exc:
            print(f"\nTiming demo failure captured: {exc}")

    if args.debug:
        query_obj.print_debug()
    else:
        query_obj.print()

    print("🖥️  ~ Done.")
