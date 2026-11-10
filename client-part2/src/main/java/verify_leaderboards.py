#!/usr/bin/env python3
"""Compare current consumer and WebSocket snapshots. Does not send swipes.
Requires Python 3.9+ and websockets 16.0. Keep load tests paused.
This verifies snapshots available now, not delivery during an earlier run.
"""
import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import urlsplit, urlunsplit
from websockets.asyncio.client import connect


def read_json(url):
    with urlopen(url, timeout=10) as response:
        return json.load(response)


def canonical(snapshot, resort, day):
    if str(snapshot['resortID']) != str(resort) or str(snapshot['dayID']) != str(day):
        raise ValueError('Wrong resort or day in snapshot')
    version = snapshot['version']
    if type(version) is not int or version < 0:
        raise ValueError('Invalid version')
    top = snapshot['top']
    if not isinstance(top, list):
        raise ValueError('Invalid top list')
    entries = []
    for index, row in enumerate(top, 1):
        if type(row['rank']) is not int or row['rank'] != index:
            raise ValueError('Invalid rank sequence')
        if type(row['vertical']) is not int:
            raise ValueError('Invalid vertical total')
        entries.append((row['rank'], str(row['skierID']), row['vertical']))
    return (str(resort), str(day), version, entries)


async def verify(args, resort):
    result = {'resortID': resort, 'dayID': args.day, 'passed': False}
    endpoint = urlsplit(args.base.rstrip('/'))
    ws_url = urlunsplit(('wss' if endpoint.scheme == 'https' else 'ws',
                        endpoint.netloc, endpoint.path + '/leaderboard/' + str(resort),
                        'dayID=' + str(args.day), ''))
    http_url = args.consumer.rstrip('/') + '/leaderboard/%s/%s' % (resort, args.day)
    try:
        before = await asyncio.to_thread(read_json, http_url)
        target = canonical(before, resort, args.day)
        result['consumerBefore'] = before
        async with connect(ws_url, open_timeout=10, ping_interval=20,
                           ping_timeout=20, max_size=1048576) as socket:
            deadline = asyncio.get_running_loop().time() + args.timeout
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError('No matching WebSocket snapshot before timeout')
                message = json.loads(await asyncio.wait_for(socket.recv(), remaining))
                received = canonical(message, resort, args.day)
                result['webSocketSnapshot'] = message
                if received == target:
                    break
        after = await asyncio.to_thread(read_json, http_url)
        result['consumerAfter'] = after
        if canonical(after, resort, args.day) != target:
            raise ValueError('Consumer changed during verification; pause traffic and rerun')
        result['version'] = before['version']
        result['entries'] = len(before['top'])
        result['passed'] = True
    except Exception as error:
        result['error'] = '%s: %s' % (type(error).__name__, error)
    print('Resort %s: %s%s' % (resort, 'PASS' if result['passed'] else 'FAIL',
          (' version=%s entries=%s' % (result['version'], result['entries']))
          if result['passed'] else ' ' + result['error']), flush=True)
    return result


async def run(args):
    baseline = await asyncio.to_thread(read_json, args.consumer.rstrip('/') + '/stats')
    results = await asyncio.gather(*(verify(args, r) for r in range(1, args.resorts + 1)))
    after = await asyncio.to_thread(read_json, args.consumer.rstrip('/') + '/stats')
    stable = baseline['uniqueSwipesProcessed'] == after['uniqueSwipesProcessed']
    passed = stable and all(r['passed'] for r in results)
    report = {'checkedAt': datetime.now(timezone.utc).isoformat(),
              'scope': 'Current snapshots; does not prove historical live delivery or latency',
              'baseUrl': args.base, 'consumerUrl': args.consumer,
              'baselineStats': baseline, 'finalStats': after,
              'consumerCountStable': stable, 'passed': passed, 'resorts': results}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + '\n')
    print('Overall: %s. Report: %s' % ('PASS' if passed else 'FAIL', output))
    if not stable:
        print('Consumer count changed: pause all traffic and wait for queues to drain.')
    return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', default='http://skitrak-alb-1256428710.us-west-2.elb.amazonaws.com/ski-servlet')
    parser.add_argument('--consumer', default='http://127.0.0.1:19090')
    parser.add_argument('--resorts', type=int, default=10)
    parser.add_argument('--day', type=int, default=1)
    parser.add_argument('--timeout', type=float, default=30)
    parser.add_argument('--output', default='../results/leaderboard-verification-' +
                        datetime.now().strftime('%Y%m%d-%H%M%S') + '.json')
    args = parser.parse_args()
    if args.resorts < 1 or args.day < 1 or args.timeout <= 0:
        parser.error('resorts, day and timeout must be positive')
    if urlsplit(args.base).scheme not in ('http', 'https'):
        parser.error('--base must use http or https')
    try:
        return asyncio.run(run(args))
    except Exception as error:
        print('Verification could not complete: %s: %s' % (type(error).__name__, error))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
