# graphdb/cli/commands/test.py


def cmd_test(args):
    print("🖥️  ~ GraphDB client CLI. Test server connectivity.")

    service = args.ctx.container.connectivity_ops

    if args.env:
        if service.test_one(args.env):
            print(f"✅ MySQL server is up and running [env='{args.env}'].")
        else:
            print(f"❌ MySQL server is down or unreachable [env='{args.env}'].")
    else:
        for env_name, ok in service.test_all().items():
            if ok:
                print(f"✅ MySQL server is up and running [env='{env_name}'].")
            else:
                print(f"❌ MySQL server is down or unreachable [env='{env_name}'].")

    print("🖥️  ~ Done.")
