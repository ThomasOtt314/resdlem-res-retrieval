"""
run_local.py -- run the Cloud Function's update on your own machine.

Uses the exact code in the function folder (imports main.py + revap.py from
--function-dir), so a local run writes what the deployed function would write.
No 60-minute limit, which suits a long backfill.

Targets (pick one):
  default        the real bucket gs://operevap-res-retrieval-elevation, using your
                 own Google credentials (one-time: gcloud auth application-default login)
  --local-dir D  a folder with the same layout (by_reservoir/, by_date/, reports/)

Usage:
  python3 run_local.py --function-dir FOLDER --mode backfill --start 19790101 --end 20261109 --source BOR --dry-run
  python3 run_local.py --function-dir FOLDER --mode backfill --start 19790101 --end 20261109 --source BOR
  python3 run_local.py --function-dir FOLDER --mode daily

--dry-run computes and reads everything but writes nothing.
Needs: pandas, numpy, requests, dataretrieval, google-cloud-storage (bucket target only).
"""
import argparse
import json
import os
import sys


class LocalStore:
    """Same interface as main.GCSStore, on a local folder."""

    def __init__(self, root):
        self.root = root
        self.name = os.path.abspath(root)

    def read(self, path):
        p = os.path.join(self.root, path)
        return open(p).read() if os.path.isfile(p) else None

    def write(self, path, text):
        p = os.path.join(self.root, path)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, 'w') as fh:
            fh.write(text)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--function-dir', required=True, help='folder with main.py, revap.py, reservoirs.csv, curves/')
    ap.add_argument('--mode', default='daily', choices=['daily', 'backfill'])
    ap.add_argument('--days', type=int, default=30, help='daily window length')
    ap.add_argument('--start', help='backfill start YYYYMMDD')
    ap.add_argument('--end', help='backfill end YYYYMMDD')
    ap.add_argument('--source', choices=['BOR', 'TWDB'], help='run only this group (default: all)')
    ap.add_argument('--only', nargs='+', help='run only these reservoir names')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--local-dir', help='write to this folder instead of the bucket')
    ap.add_argument('--workers', type=int, default=6, help='reservoirs downloaded at the same time')
    a = ap.parse_args()

    sys.path.insert(0, os.path.abspath(a.function_dir))
    import main as fn                      # the Cloud Function's main.py

    store = LocalStore(a.local_dir) if a.local_dir else fn.GCSStore(fn.BUCKET)
    if not a.dry_run and not a.local_dir:
        print(f'WRITING to {store.name} (mode={a.mode}, {a.start}..{a.end}, source={a.source or "all"})')
    summary = fn.update(store, mode=a.mode, days=a.days, start=a.start, end=a.end,
                        dry_run=a.dry_run, only=a.only, workers=a.workers, source=a.source)
    print(json.dumps(summary, indent=1))
    raise SystemExit(1 if summary['reservoirs_failed'] else 0)


if __name__ == '__main__':
    main()
