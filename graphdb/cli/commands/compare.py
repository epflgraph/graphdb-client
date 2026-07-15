# graphdb/cli/commands/compare.py
from graphdb.application.compare_service import CompareService


def _fmt_bytes(n):
    if n is None:
        return "NULL"
    try:
        n = int(n)
    except Exception:
        return str(n)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    v = float(n)
    for u in units:
        if v < 1024 or u == units[-1]:
            return f"{v:.2f} {u}"
        v /= 1024.0


def _fmt(v):
    return "NULL" if v is None else str(v)


def _print_table_result(result):
    table = result["table"]
    source_path = f"{result['source']['env']}.{result['source']['schema']}"
    target_path = f"{result['target']['env']}.{result['target']['schema']}"

    header_width = max(len(table), len(f"Source: {source_path}"), len(f"Target: {target_path}")) + 4
    bar = "─" * (header_width + 1)
    table_name_colored = f"\033[1;36m{table}\033[0m"
    print("\n")
    print(f"┌{bar}┐")
    print(f"│ Table : {table_name_colored}{' ' * (header_width - 9 - len(table))} │")
    print(f"│ Source: {source_path:<{header_width - 9}} │")
    print(f"│ Target: {target_path:<{header_width - 9}} │")
    print(f"└{bar}┘")

    for row in result["rows"]:
        metric = row["metric"]
        a = row["source"]
        b = row["target"]
        status = row["status"]
        if metric in ("data_length", "index_length", "total_bytes"):
            a = _fmt_bytes(a)
            b = _fmt_bytes(b)
        else:
            a = _fmt(a)
            b = _fmt(b)
        status_symbol = {"OK": "🟢", "WARN": "🟡", "ERR": "🔴"}.get(status, "⚪")
        print(f"{metric:<24} {a:<24} {b:<24} {status_symbol}")


def cmd_compare(args):
    print("🖥️  ~ GraphDB client CLI. Compare database or tables across servers.")

    service = CompareService(args.ctx.registry)

    if args.table_name:
        result = service.compare_tables(
            args.from_env,
            args.from_schema,
            args.to_env,
            args.to_schema,
            args.table_name,
            row_count_tolerance=args.row_count_tolerance,
            ignore_warnings=args.ignore_warnings,
        )
        if "error" in result:
            print(f"❌ {result['error']}")
        else:
            _print_table_result(result)
    else:
        results = service.compare_databases(
            args.from_env,
            args.from_schema,
            args.to_env,
            args.to_schema,
            row_count_tolerance=args.row_count_tolerance,
            ignore_warnings=args.ignore_warnings,
        )
        for result in results:
            if "error" in result:
                print(f"❌ {result['error']}")
                continue
            if args.ignore_warnings and result["has_warning"] and not result["has_error"]:
                continue
            _print_table_result(result)

    print("🖥️  ~ Done.")
