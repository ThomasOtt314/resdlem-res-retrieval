# REVAP reservoir update — Cloud Function

Keeps `gs://operevap-res-retrieval-elevation` (project `nasa-revap`) up to date with daily
stage, area, volume and mean depth for 228 operational reservoirs (117 BOR, 111 TWDB).

| File | Purpose |
|---|---|
| `main.py` | HTTP entry point `revap_update`; updates the bucket |
| `revap.py` | Fetch + processing (RISE, CDEC, Hydromet, USGS, TWDB) |
| `reservoirs.csv` | One row per reservoir: source, site/parameter codes, `start_year`, curves |
| `curves/` | Elevation–area / elevation–capacity curves (BOR) |
| `requirements.txt` | Dependencies |
| `.gcloudignore` | Deploys only the files above |

## What a call does
1. Runs the full record (1979-01-01 … today) for every reservoir — outlier checks and gap
   filling depend on the whole record.
2. Rewrites only the dates in the write window, in both copies:
   - `by_reservoir/<NAME>.csv` — full record per reservoir
   - `by_date/YYYY/MM/res_YYYYMMDD.csv` — all reservoirs per day
3. Writes `reports/run_<process_date>_<mode>.csv`.

Dates outside the window are never changed; a reservoir that fails keeps its existing rows.
Returns 200, or 500 if any reservoir failed (the others are still written).

## Parameters (query string or JSON body)
| Name | Default | Meaning |
|---|---|---|
| `mode` | `daily` | `daily`: last `days` days ending today (Pacific). `backfill`: `start`…`end` |
| `days` | `30` | Window length for `daily` |
| `start`, `end` | – | `YYYYMMDD`, for `backfill` |
| `dry_run` | `false` | Compute everything, write nothing |
| `only` | – | Comma-separated reservoir names |

Environment variables: `BUCKET` (default `operevap-res-retrieval-elevation`),
`GCP_PROJECT` (default `nasa-revap`).

## Output columns
`RES_NAME, date, stage_m, area_m2, volume_m3, depth_m, qflag, source, process_date`
— metres, m², m³; `stage_m = -9999` for TWDB; `process_date` = the run's date (YYYY-mm-dd).

`qflag`: `O` observed · `missing` BOR gap, filled from neighbouring days ·
`outlier` BOR reading rejected, filled from neighbouring days · `M` TWDB no complete
observation, forward-filled.

BOR outliers (`flag_outliers` in revap.py, settings at the top of that section), three
steps over the whole record, each on the readings the previous step kept:
1. **range** — more than 100 ft above the top or 50 ft below the bottom of the
   reservoir's own area/capacity curve (long runs of impossible values, placeholders).
2. **gross** — more than 100 ft from the median of the surrounding 31 readings.
3. **spike** — a 1–2 reading jump away from both neighbours that comes back,
   larger than max(3 ft, 5 × local day-to-day noise); only where readings are ≤ 3 days apart.

## Adding a reservoir
Add a row to `reservoirs.csv` (BOR: plus its curve files in `curves/`). No code changes.

## Deployment
Not deployed yet — this section will be filled in once it is.
