"""
revap.py -- REVAP reservoir pipeline (BOR + TWDB)

Everything it needs sits next to it:
    reservoirs.csv   one row per reservoir: where to fetch it, how to convert it
    curves/          elevation-area / elevation-capacity curves (BOR only)

Used by main.py (the Cloud Function): run() returns one DataFrame with
    RES_NAME, date, stage_m, area_m2, volume_m3, depth_m, qflag, source, process_date
and a per-reservoir report (status, rows, flags, seconds).

reservoirs.csv columns:
    RES_NAME     reservoir name (unique)
    source       RISE | CDEC | CPN_Hydromet | MB_Hydromet | USGS | TWDB
    site         RISE item_id | station id | USGS site number | TWDB url name
    param        CDEC sensor | Hydromet pcode | USGS parameter code
    stat         CDEC duration code | USGS statistic code
                 (USGS with no stat = 15-min data averaged per local day)
    tz           time zone for that local day (USGS 15-min only)
    start_year   drop rows before Jan 1 of this year
                 (BOR: record start; TWDB: impoundment year)
    curve_type   ACAP32 | Table      (BOR only)
    area_curve   file in curves/ (without .csv)
    volume_curve file in curves/ (without .csv)
"""
import csv
import datetime as dt
import os
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
TIMEOUT = 120                     # seconds per HTTP request
TRIES = 3                         # attempts per reservoir
FT_TO_M, AF_TO_M3, AC_TO_M2 = 0.3048, 1233.48, 4046.86
COLS = ['RES_NAME', 'date', 'stage_m', 'area_m2', 'volume_m3',
        'depth_m', 'qflag', 'source']

_lock = threading.Lock()


def log(msg):
    with _lock:
        print(f'{dt.datetime.now():%H:%M:%S} {msg}', flush=True)


def num(values):
    """Text -> float. Blank or unparseable -> NaN."""
    s = pd.Series(values, dtype='object').astype(str).str.strip()
    return pd.to_numeric(s.replace({'': np.nan, 'nan': np.nan, 'None': np.nan}), errors='coerce')


def get(url, **kw):
    r = requests.get(url, timeout=TIMEOUT, **kw)
    r.raise_for_status()
    return r


# ============================================================================
# Fetch: each returns DataFrame[Date 'YYYY-MM-DD', Value]
# ============================================================================

def fetch_rise(res, s, e):
    heads = {'Accept': 'application/vnd.api+json'}
    page = get('https://data.usbr.gov/rise/api/result', headers=heads,
               params={'itemId': int(res.site),
                       'dateTime[before]': e + dt.timedelta(days=1),
                       'dateTime[after]': s,
                       'itemsPerPage': 10000}).json()
    recs = [d['attributes'] for d in page.get('data', [])]
    while 'next' in page.get('links', {}):                 # all pages
        page = get('https://data.usbr.gov' + page['links']['next'], headers=heads).json()
        recs += [d['attributes'] for d in page.get('data', [])]
    df = pd.DataFrame(recs, columns=['dateTime', 'result']) if recs else pd.DataFrame(columns=['dateTime', 'result'])
    return pd.DataFrame({'Date': df['dateTime'].astype(str).str.split('T').str[0],
                         'Value': df['result']})


def fetch_cdec(res, s, e):
    recs = get('https://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet',
               params={'Stations': res.site, 'SensorNums': str(int(res.param)),
                       'dur_code': res.stat, 'Start': s, 'End': e}).json()
    if not recs:
        return pd.DataFrame(columns=['Date', 'Value'])
    df = pd.DataFrame(recs)
    df = df[df['value'] >= 0]                                # -9999 = missing
    d = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M')
    df = df.assign(Date=d.dt.strftime('%Y-%m-%d'))[(d >= s) & (d <= e)]
    return df.groupby('Date')['value'].mean().rename('Value').reset_index()


def fetch_cpn(res, s, e):
    text = get('https://www.usbr.gov/pn-bin/daily.pl?station={}&format=csv&year={}&month={}&day={}'
               '&year={}&month={}&day={}&pcode={}'.format(res.site, s.year, s.month, s.day,
                                                          e.year, e.month, e.day, res.param)).text
    lines = text.split('\n')[1:-1]                          # drop header + trailing
    return pd.DataFrame([ln.split(',')[:2] for ln in lines], columns=['Date', 'Value'])


def fetch_mb(res, s, e):
    text = get('https://www.usbr.gov/gp-bin/webarccsv.pl?parameter={}%20{}&syer={}&smnth={}&sdy={}'
               '&eyer={}&emnth={}&edy={}&format=2'.format(res.site, res.param, s.year, s.month,
                                                         s.day, e.year, e.month, e.day)).text
    lines = text.split('\r\n')[22:-5]                        # strip page header/footer
    df = pd.DataFrame([ln.split(',')[:2] for ln in lines], columns=['Date', 'Value'])
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
    return df


def fetch_usgs(res, s, e):
    import dataretrieval.waterdata as wd
    loc = f'USGS-{int(res.site):08d}'
    pcode = f'{int(res.param):05d}'

    if pd.notna(res.stat) and str(res.stat) != '':           # daily values
        df, _ = wd.get_daily(monitoring_location_id=loc, parameter_code=pcode,
                             statistic_id=f'{int(res.stat):05d}',
                             time=f'{s:%Y-%m-%d}/{e:%Y-%m-%d}', skip_geometry=True)
        if len(df) == 0:
            return pd.DataFrame(columns=['Date', 'Value'])
        return pd.DataFrame({'Date': pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d').values,
                             'Value': df['value'].values})

    # 15-minute values -> mean per LOCAL day. Max 3 years per request, so one
    # request per calendar year, starting where the series begins.
    first = s
    try:
        md, _ = wd.get_time_series_metadata(monitoring_location_id=loc, parameter_code=pcode)
        b = pd.to_datetime(md['begin'], utc=True).min()
        if pd.notna(b):
            first = max(s, b.tz_convert(None).to_pydatetime() - dt.timedelta(days=1))
    except Exception as ex:
        log(f'  {res.RES_NAME}: series start lookup failed ({type(ex).__name__}), fetching from {s:%Y}')
    a, stop, parts = first - dt.timedelta(days=1), e + dt.timedelta(days=2), []
    while a < stop:
        b = min(dt.datetime(a.year + 1, 1, 1), stop)
        df, _ = wd.get_continuous(monitoring_location_id=loc, parameter_code=pcode,
                                  time=f'{a:%Y-%m-%d}/{b:%Y-%m-%d}')
        if len(df):
            parts.append(df[['time', 'value']])
        a = b
    if not parts:
        return pd.DataFrame(columns=['Date', 'Value'])
    df = pd.concat(parts).drop_duplicates()
    day = pd.to_datetime(df['time'], utc=True).dt.tz_convert(res.tz).dt.strftime('%Y-%m-%d')
    out = pd.to_numeric(df['value']).groupby(day.values).mean()
    return pd.DataFrame({'Date': out.index, 'Value': out.values})


def fetch_twdb(res, s, e):
    text = get(f'https://www.waterdatafortexas.org/reservoirs/individual/{res.site}.csv').content.decode('utf-8')
    # Header = first non-comment row starting with 'date'. Columns are taken by
    # name, so TWDB adding/dropping/reordering other columns doesn't matter
    # (Caddo Lake dropped dead_pool_capacity in Nov 2026).
    rows = [r for r in csv.reader(text.splitlines()) if r and not r[0].startswith('#')]
    i = next((k for k, r in enumerate(rows) if r[0].strip() == 'date'), None)
    if i is None:
        raise ValueError(f'TWDB {res.site}: no header row starting with "date"')
    header = [c.strip() for c in rows[i]]
    need = ['date', 'surface_area', 'reservoir_storage']
    missing = [c for c in need if c not in header]
    if missing:
        raise ValueError(f'TWDB {res.site}: columns {missing} not in header {header}')
    body = [r for r in rows[i + 1:] if len(r) == len(header)]
    return pd.DataFrame(body, columns=header)[need]


FETCH = {'RISE': fetch_rise, 'CDEC': fetch_cdec, 'CPN_HYDROMET': fetch_cpn,
         'MB_HYDROMET': fetch_mb, 'USGS': fetch_usgs, 'TWDB': fetch_twdb}


# ============================================================================
# BOR: elevation -> daily series -> area & volume
# ============================================================================

# Outlier settings (feet / observations / days). See flag_outliers().
RANGE_ABOVE_FT = 100.0   # allowed above the top of the curve
RANGE_BELOW_FT = 50.0    # allowed below the bottom of the curve
GROSS_WINDOW = 31        # observations in the centred median
GROSS_FT = 100.0         # allowed distance from that median
SPIKE_K = 5.0            # spike threshold = K * local day-to-day noise ...
SPIKE_FLOOR_FT = 3.0     # ... but never below this
NOISE_WINDOW = 31        # observations used for the local noise
SPIKE_MAX_LEN = 2        # longest run of observations treated as a spike
SPIKE_MAX_GAP = 3        # days to the neighbours, else the spike test is skipped


def _spikes(day, x):
    """Runs of 1..SPIKE_MAX_LEN observations above both neighbours (or below
    both) by more than the threshold, where the level comes back afterwards.
    day: int days, x: elevations, both sorted, no NaN. Returns a bool array."""
    n = len(x)
    flag = np.zeros(n, bool)
    step = np.r_[np.nan, np.abs(np.diff(x))]
    noise = pd.Series(step).rolling(NOISE_WINDOW, center=True, min_periods=5).median().to_numpy()
    thr = np.maximum(SPIKE_FLOOR_FT, SPIKE_K * np.nan_to_num(noise, nan=0.0))
    for L in range(1, SPIKE_MAX_LEN + 1):
        for t in range(1, n - L):
            if day[t] - day[t - 1] > SPIKE_MAX_GAP or day[t + L] - day[t + L - 1] > SPIKE_MAX_GAP:
                continue                                     # sparse readings: not judged
            b, a, seg, th = x[t - 1], x[t + L], x[t:t + L], thr[t]
            beyond = max((seg - max(a, b)).min(),            # above both neighbours by ...
                         (min(a, b) - seg).min())            # ... or below both by
            if beyond > th and abs(a - b) < th:              # and the level comes back
                flag[t:t + L] = True
    return flag


def flag_outliers(dates, elev, bot, top):
    """Three steps, each on the readings the previous steps kept:
      1. range  above curve top + RANGE_ABOVE_FT or below curve bottom - RANGE_BELOW_FT
      2. gross  more than GROSS_FT from the median of the surrounding GROSS_WINDOW readings
      3. spike  short jump away from both neighbours that comes back (_spikes)
    dates: 'YYYY-MM-DD' strings, elev: floats, both sorted by date, no NaN.
    Returns a bool array (True = outlier)."""
    x = np.asarray(elev, float)
    day = pd.to_datetime(pd.Series(dates)).to_numpy('datetime64[D]').astype(np.int64)
    flag = (x > top + RANGE_ABOVE_FT) | (x < bot - RANGE_BELOW_FT)
    keep = np.flatnonzero(~flag)
    if len(keep):
        med = pd.Series(x[keep]).rolling(GROSS_WINDOW, center=True,
                                         min_periods=GROSS_WINDOW // 4 + 1).median().to_numpy()
        flag[keep[np.abs(x[keep] - med) > GROSS_FT]] = True
    keep = np.flatnonzero(~flag)
    if len(keep) > 2:
        flag[keep[_spikes(day[keep], x[keep])]] = True
    return flag


def daily_elevation(raw, res, s, e):
    """Raw readings -> one elevation per day (ft), with qflag O / outlier / missing."""
    df = raw.drop_duplicates(subset='Date', keep='last').copy()
    df['elevation'] = num(df['Value']).values
    df = df[pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce').notna()]
    df = df.dropna(subset=['elevation']).sort_values('Date')[['Date', 'elevation']]
    df['qflag'] = 'O'
    # outliers over the whole fetched record -> blank + flag
    bot, top = curve_limits(res)
    bad = flag_outliers(df['Date'], df['elevation'], bot, top)
    df.loc[bad, 'qflag'] = 'outlier'
    df.loc[bad, 'elevation'] = np.nan
    # every day in range; gaps flagged 'missing'; fill forward then backward
    days = pd.DataFrame({'Date': pd.date_range(s, e).strftime('%Y-%m-%d')})
    df = days.merge(df, on='Date', how='outer').sort_values('Date')
    df['qflag'] = df['qflag'].fillna('missing')
    df['elevation'] = df['elevation'].ffill().bfill()
    keep = (df['Date'] >= f'{int(res.start_year)}-01-01') & (df['Date'] <= f'{e:%Y-%m-%d}')
    return df[keep].reset_index(drop=True)


_curves = {}


def curve(name, value_col=None):
    """Load a curve file once. Table curves: drop blank rows (values above the
    top of the table then clamp to the top value)."""
    with _lock:
        if (name, value_col) not in _curves:
            t = pd.read_csv(os.path.join(HERE, 'curves', name + '.csv'), thousands=',')
            _curves[(name, value_col)] = t.dropna(subset=['Elevation', value_col]) if value_col else t
        return _curves[(name, value_col)]


def acap32(tbl, elev, kind):
    """ACAP32 piecewise quadratic, segment = highest Elev_Base <= elevation.
    area = 2*A3*d + A2 ; capacity = A3*d^2 + A2*d + A1 ; d = elev - base.
    NaN below the lowest segment."""
    order = np.argsort(tbl['Elev_Base'].to_numpy(), kind='stable')
    base, A1, A2, A3 = (tbl[c].to_numpy()[order].astype(float) for c in ('Elev_Base', 'A1', 'A2', 'A3'))
    out = np.full(elev.shape, np.nan)
    ok = ~np.isnan(elev)
    idx = np.full(elev.shape, -1)
    idx[ok] = np.searchsorted(base, elev[ok], side='right') - 1
    ok &= idx >= 0
    if ok.any():
        k = np.searchsorted(base, base[idx[ok]], side='left')   # first row on ties
        d = elev[ok] - base[k]
        out[ok] = 2 * A3[k] * d + A2[k] if kind == 'area' else A3[k] * d ** 2 + A2[k] * d + A1[k]
    return out


def lookup(res, elev, kind):
    name = res.area_curve if kind == 'area' else res.volume_curve
    if res.curve_type == 'ACAP32':
        return acap32(curve(name), elev, kind)
    col = 'Area' if kind == 'area' else 'Capacity'
    t = curve(name, col)
    v = np.interp(elev, t['Elevation'].to_numpy(float), t[col].to_numpy(float))
    return np.where(np.isnan(elev), np.nan, v)


def curve_limits(res):
    """(bottom, top) elevation covered by both the area and the capacity curve.
    Table: lowest / highest Elevation with a value.
    ACAP32: lowest Elev_Base / highest Elev_Base + the last step between bases."""
    lims = []
    for name, col in ((res.area_curve, 'Area'), (res.volume_curve, 'Capacity')):
        if res.curve_type == 'ACAP32':
            base = np.sort(curve(name)['Elev_Base'].astype(float).unique())
            step = base[-1] - base[-2] if len(base) > 1 else 0.0
            lims.append((base[0], base[-1] + step))
        else:
            el = curve(name, col)['Elevation'].astype(float)
            lims.append((el.min(), el.max()))
    return max(l[0] for l in lims), min(l[1] for l in lims)


def process_bor(raw, res, s, e):
    d = daily_elevation(raw, res, s, e)
    elev = d['elevation'].to_numpy(float)
    out = pd.DataFrame({'RES_NAME': res.RES_NAME, 'date': d['Date'].values,
                        'stage_m': elev * FT_TO_M,
                        'area_m2': lookup(res, elev, 'area') * AC_TO_M2,
                        'volume_m3': lookup(res, elev, 'volume') * AF_TO_M3,
                        'qflag': d['qflag'].values, 'source': 'BOR'})
    return out


# ============================================================================
# TWDB: area & storage come directly from the source
# ============================================================================

def process_twdb(raw, res, s, e):
    df = pd.DataFrame({'date': pd.to_datetime(raw['date']),
                       'area_m2': num(raw['surface_area']).values * AC_TO_M2,
                       'volume_m3': num(raw['reservoir_storage']).values * AF_TO_M3})
    df = df.dropna(subset=['date']).drop_duplicates('date', keep='last').sort_values('date')
    df['qflag'] = np.where(df['area_m2'].notna() & df['volume_m3'].notna(), 'O', 'M')
    with np.errstate(divide='ignore', invalid='ignore'):
        df['depth_m'] = df['volume_m3'] / df['area_m2']      # before filling, as before
    days = pd.DataFrame({'date': pd.date_range(df['date'].min(), df['date'].max())})
    df = days.merge(df, on='date', how='left')
    df['qflag'] = df['qflag'].fillna('M')                    # missing days
    df[['area_m2', 'volume_m3', 'depth_m']] = df[['area_m2', 'volume_m3', 'depth_m']].ffill()
    df = df[(df['date'] >= s) & (df['date'] <= e)]
    if pd.notna(res.start_year) and str(res.start_year) != '':   # impoundment year
        df = df[df['date'] >= f'{int(res.start_year)}-01-01']
    return pd.DataFrame({'RES_NAME': res.RES_NAME, 'date': df['date'].dt.strftime('%Y-%m-%d').values,
                         'stage_m': -9999.0, 'area_m2': df['area_m2'].values,
                         'volume_m3': df['volume_m3'].values, 'depth_m': df['depth_m'].values,
                         'qflag': df['qflag'].values, 'source': 'TWDB'})


# ============================================================================
# Run
# ============================================================================

def run_one(res, s, e):
    src = res.source.upper()
    for attempt in range(1, TRIES + 1):
        try:
            raw = FETCH[src](res, s, e)
            if src == 'TWDB':
                df = process_twdb(raw, res, s, e)
            else:
                if num(raw['Value']).notna().sum() == 0:
                    raise RuntimeError('no valid elevations returned')
                df = process_bor(raw, res, s, e)
                with np.errstate(divide='ignore', invalid='ignore'):
                    df['depth_m'] = df['volume_m3'] / df['area_m2']
            return df.replace([np.inf, -np.inf], np.nan)[COLS], len(raw)
        except Exception as ex:
            if attempt == TRIES:
                raise
            log(f'  {res.RES_NAME}: attempt {attempt} failed ({type(ex).__name__}: {str(ex)[:100]}); retrying')
            time.sleep(5 * attempt)


def run(reservoirs, s, e, workers=6, process_date=None):
    """process_date: YYYY-mm-dd string written to every row (default: today)."""
    process_date = process_date or dt.date.today().strftime('%Y-%m-%d')
    dt.datetime.strptime(process_date, '%Y-%m-%d')            # fail early on a bad format
    log(f'{len(reservoirs)} reservoirs, {s:%Y-%m-%d}..{e:%Y-%m-%d}, {workers} at a time')
    frames, report = [], []

    def job(res):
        t0 = time.time()
        rec = {'RES_NAME': res.RES_NAME, 'source': res.source}
        try:
            df, n_raw = run_one(res, s, e)
            q = df['qflag'].value_counts()
            rec.update(status='OK', raw_rows=n_raw, rows=len(df), first=df['date'].min(),
                       last=df['date'].max(), n_O=q.get('O', 0),
                       n_missing=q.get('missing', 0) + q.get('M', 0), n_outlier=q.get('outlier', 0),
                       n_nan_area=int(df['area_m2'].isna().sum()),
                       n_nan_volume=int(df['volume_m3'].isna().sum()))
        except Exception as ex:
            df = None
            rec['status'] = f'FAILED {type(ex).__name__}: {str(ex)[:200]}'
            traceback.print_exc()
        rec['seconds'] = round(time.time() - t0, 1)
        log(f'{res.source:<13} {res.RES_NAME:<36} {rec["seconds"]:6.1f}s  {rec["status"][:70]}')
        return df, rec

    with ThreadPoolExecutor(workers) as pool:
        for fut in as_completed([pool.submit(job, r) for r in reservoirs.itertuples(index=False)]):
            df, rec = fut.result()
            report.append(rec)
            if df is not None:
                frames.append(df)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLS)
    combined['process_date'] = process_date                  # string, YYYY-mm-dd
    combined = combined.sort_values(['date', 'source', 'RES_NAME'], ignore_index=True)
    return combined, pd.DataFrame(report).sort_values(['source', 'RES_NAME'], ignore_index=True)


def load_reservoirs(path=os.path.join(HERE, 'reservoirs.csv')):
    r = pd.read_csv(path, dtype={'site': str, 'param': str, 'stat': str})
    if r['RES_NAME'].duplicated().any():
        raise ValueError(f'duplicate RES_NAME in {path}: {sorted(r.RES_NAME[r.RES_NAME.duplicated()])}')
    return r
