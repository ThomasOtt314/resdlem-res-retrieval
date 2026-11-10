"""
main.py -- Cloud Function (2nd gen, HTTP) that keeps the bucket up to date.

Each call:
  1. runs revap.py over the FULL record (1979-01-01 .. today), so outlier checks
     and gap filling always see the whole record
  2. takes the rows inside the WRITE WINDOW
       daily    (default): the last 30 days, ending today
       backfill: ?start=YYYYMMDD&end=YYYYMMDD
  3. updates both copies in the bucket, replacing only the window's dates:
       by_reservoir/<NAME>.csv          read -> drop window dates -> add new -> write
       by_date/YYYY/MM/res_YYYYMMDD.csv read -> replace rows of reservoirs that ran OK -> write
     History outside the window is never touched. A reservoir that FAILED keeps
     its existing rows everywhere.
  4. writes reports/run_<process_date>.csv (one line per reservoir)
Returns HTTP 200 if all reservoirs ran, 500 if any failed (data for the others
is still written), with a JSON summary.

Request parameters (query string or JSON body), all optional:
    mode      daily | backfill               (default daily)
    days      window length for daily        (default 30)
    start,end YYYYMMDD window for backfill
    dry_run   true -> compute everything, write nothing
    only      comma-separated reservoir names (testing)
    source    BOR | TWDB: run only that group (BOR = every non-TWDB source)

"""
import datetime as dt
import io
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

import pandas as pd

import revap

BUCKET = os.environ.get('BUCKET', 'operevap-res-retrieval-elevation')
PROJECT = os.environ.get('GCP_PROJECT', 'nasa-revap')
TZ = ZoneInfo('America/Los_Angeles')     # "today" and process_date are Pacific dates
RECORD_START = dt.datetime(1979, 1, 1)
DAILY_DAYS = 30
IO_WORKERS = 16
COLS = revap.COLS + ['process_date']


# ============================================================================
# Storage
# ============================================================================

class GCSStore:
    def __init__(self, bucket):
        from google.cloud import storage
        self.bucket = storage.Client(project=PROJECT).bucket(bucket)
        self.name = f'gs://{bucket}'

    def read(self, path):
        blob = self.bucket.blob(path)
        return blob.download_as_text() if blob.exists() else None

    def write(self, path, text):
        self.bucket.blob(path).upload_from_string(text, content_type='text/csv')


def res_path(name):
    return f'by_reservoir/{revap_safe(name)}.csv'


def day_path(date):          # date 'YYYY-MM-DD'
    return f'by_date/{date[:4]}/{date[5:7]}/res_{date.replace("-", "")}.csv'


def revap_safe(name):
    """Same file naming as build_archive.py: LAKE MEAD -> LAKE_MEAD."""
    import re
    return re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_')


def read_csv(text):
    df = pd.read_csv(io.StringIO(text), dtype={'RES_NAME': str, 'date': str, 'qflag': str,
                                               'source': str, 'process_date': str})
    for c in COLS:                       # older files may lack a column (e.g. process_date)
        if c not in df:
            df[c] = pd.NA
    return df[COLS]


def to_csv(df):
    return df[COLS].to_csv(index=False)


# ============================================================================
# Update logic
# ============================================================================

def update_reservoir(store, name, new_rows, w0, w1, dry_run):
    """Replace window dates in one reservoir's file. Returns (rows_before, rows_after)."""
    path = res_path(name)
    text = store.read(path)
    old = read_csv(text) if text else pd.DataFrame(columns=COLS)
    keep = old[(old['date'] < w0) | (old['date'] > w1)]
    out = pd.concat([keep, new_rows[COLS]], ignore_index=True).sort_values('date', ignore_index=True)
    if out['date'].duplicated().any():
        raise ValueError(f'{name}: duplicate dates after merge')
    if not dry_run:
        store.write(path, to_csv(out))
    return len(old), len(out)


def update_day(store, date, new_rows, ok_names, dry_run):
    """Replace rows of reservoirs that ran OK in one day file; keep the rest."""
    path = day_path(date)
    text = store.read(path)
    old = read_csv(text) if text else pd.DataFrame(columns=COLS)
    keep = old[~old['RES_NAME'].isin(ok_names)]
    out = pd.concat([keep, new_rows[COLS]], ignore_index=True)
    out = out.sort_values(['source', 'RES_NAME'], ignore_index=True)
    if out['RES_NAME'].duplicated().any():
        raise ValueError(f'{date}: duplicate reservoirs after merge')
    if not dry_run:
        store.write(path, to_csv(out))
    return len(old), len(out)


def update(store, mode='daily', days=DAILY_DAYS, start=None, end=None, dry_run=False,
           only=None, workers=6, today=None, source=None):
    t0 = time.time()
    today = today or dt.datetime.now(TZ).date()
    process_date = today.strftime('%Y-%m-%d')
    if mode == 'daily':
        w1 = today
        w0 = today - dt.timedelta(days=int(days) - 1)
    elif mode == 'backfill':
        if not (start and end):
            raise ValueError('backfill needs start and end (YYYYMMDD)')
        w0 = dt.datetime.strptime(start, '%Y%m%d').date()
        w1 = dt.datetime.strptime(end, '%Y%m%d').date()
    else:
        raise ValueError(f'unknown mode {mode}')
    if w0 > w1:
        raise ValueError(f'window start {w0} is after end {w1}')
    w0s, w1s = w0.strftime('%Y-%m-%d'), w1.strftime('%Y-%m-%d')
    run_end = dt.datetime.combine(max(w1, today), dt.time())
    revap.log(f'mode={mode} window {w0s}..{w1s} process_date={process_date} '
              f'store={store.name} dry_run={dry_run} source={source or "all"}')

    # 1. full-record run
    res = revap.load_reservoirs()
    if only:
        res = res[res['RES_NAME'].isin(only)]
    if source:
        if source.upper() not in ('BOR', 'TWDB'):
            raise ValueError(f'source must be BOR or TWDB, not {source}')
        res = res[(res['source'].str.upper() == 'TWDB') == (source.upper() == 'TWDB')]
    combined, report = revap.run(res, RECORD_START, run_end, workers, process_date)
    ok_names = set(report.loc[report['status'] == 'OK', 'RES_NAME'])
    failed = report.loc[report['status'] != 'OK', ['RES_NAME', 'status']]

    # 2. window rows only
    win = combined[(combined['date'] >= w0s) & (combined['date'] <= w1s)]
    win = win[win['RES_NAME'].isin(ok_names)]
    revap.log(f'window rows: {len(win):,} for {win["RES_NAME"].nunique()} reservoirs, '
              f'{win["date"].nunique()} dates')

    # 3a. per-reservoir files
    by_res = {n: g for n, g in win.groupby('RES_NAME')}
    with ThreadPoolExecutor(IO_WORKERS) as pool:
        res_out = dict(zip(by_res, pool.map(
            lambda n: update_reservoir(store, n, by_res[n], w0s, w1s, dry_run), by_res)))
    # 3b. per-date files
    by_day = {d: g for d, g in win.groupby('date')}
    with ThreadPoolExecutor(IO_WORKERS) as pool:
        day_out = dict(zip(by_day, pool.map(
            lambda d: update_day(store, d, by_day[d], ok_names, dry_run), by_day)))

    # 4. report
    report['window_rows'] = report['RES_NAME'].map(win.groupby('RES_NAME').size()).fillna(0).astype(int)
    report['file_rows_before'] = report['RES_NAME'].map({k: v[0] for k, v in res_out.items()})
    report['file_rows_after'] = report['RES_NAME'].map({k: v[1] for k, v in res_out.items()})
    report['process_date'] = process_date
    if not dry_run:
        store.write(f'reports/run_{process_date}_{mode}.csv', report.to_csv(index=False))

    summary = dict(mode=mode, window=[w0s, w1s], process_date=process_date, dry_run=dry_run,
                   reservoirs_ok=len(ok_names), reservoirs_failed=failed.to_dict('records'),
                   reservoir_files=len(res_out), day_files=len(day_out),
                   seconds=round(time.time() - t0))
    revap.log('SUMMARY ' + json.dumps(summary))
    return summary


# ============================================================================
# Cloud Function entry point
# ============================================================================

def revap_update(request):
    """HTTP entry point (functions-framework)."""
    args = dict(request.args or {})
    body = request.get_json(silent=True) or {}
    args.update(body)
    try:
        summary = update(
            GCSStore(BUCKET),
            mode=args.get('mode', 'daily'),
            days=int(args.get('days', DAILY_DAYS)),
            start=args.get('start'), end=args.get('end'),
            dry_run=str(args.get('dry_run', 'false')).lower() in ('1', 'true', 'yes'),
            only=[s.strip() for s in args['only'].split(',')] if args.get('only') else None,
            source=args.get('source'))
    except Exception as ex:
        revap.log(f'ERROR {type(ex).__name__}: {ex}')
        return (json.dumps({'error': f'{type(ex).__name__}: {ex}'}), 500,
                {'Content-Type': 'application/json'})
    code = 500 if summary['reservoirs_failed'] else 200
    return json.dumps(summary), code, {'Content-Type': 'application/json'}
