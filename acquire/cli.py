"""命令行：一轮队列采集与状态查询。

.. code-block:: console

    python -m acquire.service pull   URL DATABASE [--limit N] [--lease-seconds N]
    python -m acquire.service status DATABASE [KEY] [--status STATUS]
    python -m acquire.service pending DATABASE
    python -m acquire.service messages DATABASE [KEY]

``URL`` 是队列根（样例 ``http://host/queue``），也接受入口 ``/queue/pull``。
"""

import argparse
import json
import sys

from .service import (collect, transaction_status, unacknowledged_messages)


def _tasks(parser):
    sub = parser.add_subparsers(dest='task', required=True)
    pull = sub.add_parser('pull', help='领取一轮消息并处理')
    pull.add_argument('url')
    pull.add_argument('database')
    pull.add_argument('--limit', type=int, default=10)
    pull.add_argument('--lease-seconds', type=int, default=30)
    pull.add_argument('--renew-before', type=int, default=5)
    status = sub.add_parser('status', help='查询事务可用性')
    status.add_argument('database')
    status.add_argument('key', nargs='?')
    status.add_argument('--status', default=None)
    pending = sub.add_parser('pending', help='查询尚未确认的事务')
    pending.add_argument('database')
    messages = sub.add_parser('messages', help='查询未确认消息及等待原因')
    messages.add_argument('database')
    messages.add_argument('key', nargs='?')
    return sub


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    argv = list(argv)
    # 兼容原有的一次性读取调用：python -m acquire.service URL DATABASE [--method]
    if argv and argv[0] not in ('pull', 'status', 'pending', 'messages'):
        legacy = argparse.ArgumentParser(prog='acquire.service')
        legacy.add_argument('url')
        legacy.add_argument('database')
        legacy.add_argument('--method', default='GET')
        args = legacy.parse_args(argv)
        from .service import collect_one
        result = collect_one(args.url, args.database, method=args.method)
        json.dump(result, sys.stdout, ensure_ascii=False)
        sys.stdout.write('\n')
        return
    parser = argparse.ArgumentParser(prog='acquire.service')
    _tasks(parser)
    args = parser.parse_args(argv)
    if args.task == 'pull':
        result = collect(args.url, args.database, limit=args.limit,
                         lease_seconds=args.lease_seconds,
                         renew_before=args.renew_before)
    elif args.task == 'status':
        result = transaction_status(args.database, args.key, status=args.status)
    elif args.task == 'pending':
        from .store import Store
        store = Store(args.database)
        try:
            result = store.pending_transactions()
        finally:
            store.close()
    else:
        result = unacknowledged_messages(args.database, args.key)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write('\n')


if __name__ == '__main__':
    main()
