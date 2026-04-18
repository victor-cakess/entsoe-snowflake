import argparse
import logging
import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import threading
import time
from datetime import datetime
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from tqdm import tqdm
import time

load_dotenv()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# setup logging to file only
log_dir = os.path.join(BASE_DIR, "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file)]
)
logger = logging.getLogger(__name__)

API_TOKEN = os.getenv("API_TOKEN")
BASE_URL = "https://web-api.tp.entsoe.eu/api"

NS_GL  = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
NS_PUB = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

ZONES = {
    "10Y1001A1001A82H": "DE-LU",
    "10YDK-1--------W": "DK1",
    "10YDK-2--------M": "DK2",
    "10YFI-1--------U": "FI",
    "10YFR-RTE------C": "FR",
    "10YNL----------L": "NL",
    "10YBE----------2": "BE",
    "10YCH-SWISSGRIDZ": "CH",
    "10YAT-APG------L": "AT",
    "10YES-REE------0": "ES",
    "10YPT-REN------W": "PT",
    "10YGR-HTSO-----Y": "GR",
    "10YPL-AREA-----S": "PL",
    "10YCZ-CEPS-----N": "CZ",
    "10YSK-SEPS-----K": "SK",
    "10YHU-MAVIR----U": "HU",
    "10YRO-TEL------P": "RO",
    "10YSI-ELES-----O": "SI",
    "10YHR-HEP------M": "HR",
    "10YMK-MEPSO----8": "MK",
    "10YLV-1001A00074": "LV",
    "10YLT-1001A0008Q": "LT",
}

PSR_TYPES = {
    "B01": "biomass_mw",
    "B02": "fossil_lignite_mw",
    "B03": "fossil_coal_gas_mw",
    "B04": "fossil_gas_mw",
    "B05": "fossil_hard_coal_mw",
    "B06": "fossil_oil_mw",
    "B09": "geothermal_mw",
    "B10": "hydro_pumped_storage_mw",
    "B11": "hydro_run_of_river_mw",
    "B12": "hydro_reservoir_mw",
    "B14": "nuclear_mw",
    "B15": "other_renewable_mw",
    "B16": "solar_mw",
    "B17": "waste_mw",
    "B18": "wind_offshore_mw",
    "B19": "wind_onshore_mw",
    "B20": "other_mw",
    "B25": "energy_storage_mw",
}


def month_boundaries(year, month):
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    start = f"{year}{month:02d}010000"
    end = f"{next_year}{next_month:02d}010000"
    return start, end


def fetch(params, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(BASE_URL, params={"securityToken": API_TOKEN, **params}, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt * 10
                logger.warning(f"Rate limited, waiting {wait}s before retry")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {e} — retrying in {wait}s")
            time.sleep(wait)
    raise Exception(f"All {retries} attempts failed for params: {params}")


def parse_gl(response, zone_name, value_col):
    root = ET.fromstring(response.content)
    rows = []
    for ts in root.findall("ns:TimeSeries", NS_GL):
        for period in ts.findall("ns:Period", NS_GL):
            start = pd.to_datetime(period.find("ns:timeInterval/ns:start", NS_GL).text)
            resolution = period.find("ns:resolution", NS_GL).text
            minutes = int(resolution.replace("PT", "").replace("M", "").replace("H", "")) * (60 if "H" in resolution else 1)
            for point in period.findall("ns:Point", NS_GL):
                pos = int(point.find("ns:position", NS_GL).text)
                qty = float(point.find("ns:quantity", NS_GL).text)
                rows.append({
                    "timestamp": start + pd.Timedelta(minutes=minutes * (pos - 1)),
                    "zone": zone_name,
                    value_col: qty,
                })
    return pd.DataFrame(rows)


def parse_prices(response, zone_name):
    root = ET.fromstring(response.content)
    rows = []
    for ts in root.findall("ns:TimeSeries", NS_PUB):
        classification_el = ts.find("ns:classificationSequence_AttributeInstanceComponent.position", NS_PUB)
        if classification_el is not None and classification_el.text != "2":
            continue
        for period in ts.findall("ns:Period", NS_PUB):
            start = pd.to_datetime(period.find("ns:timeInterval/ns:start", NS_PUB).text)
            resolution = period.find("ns:resolution", NS_PUB).text
            minutes = int(resolution.replace("PT", "").replace("M", "").replace("H", "")) * (60 if "H" in resolution else 1)
            for point in period.findall("ns:Point", NS_PUB):
                pos = int(point.find("ns:position", NS_PUB).text)
                price = float(point.find("ns:price.amount", NS_PUB).text)
                rows.append({
                    "timestamp": start + pd.Timedelta(minutes=minutes * (pos - 1)),
                    "zone": zone_name,
                    "price_eur_mwh": price,
                })
    return pd.DataFrame(rows)


def parse_generation(response, zone_name, forecast=False):
    root = ET.fromstring(response.content)
    rows = {}
    for ts in root.findall("ns:TimeSeries", NS_GL):
        psr = ts.find("ns:MktPSRType/ns:psrType", NS_GL)
        if psr is None or psr.text not in PSR_TYPES:
            continue
        base_col = PSR_TYPES[psr.text]
        col = base_col.replace("_mw", "_forecast_mw") if forecast else base_col
        for period in ts.findall("ns:Period", NS_GL):
            start = pd.to_datetime(period.find("ns:timeInterval/ns:start", NS_GL).text)
            resolution = period.find("ns:resolution", NS_GL).text
            minutes = int(resolution.replace("PT", "").replace("M", "").replace("H", "")) * (60 if "H" in resolution else 1)
            for point in period.findall("ns:Point", NS_GL):
                pos = int(point.find("ns:position", NS_GL).text)
                qty = float(point.find("ns:quantity", NS_GL).text)
                ts_key = start + pd.Timedelta(minutes=minutes * (pos - 1))
                if ts_key not in rows:
                    rows[ts_key] = {"timestamp": ts_key, "zone": zone_name}
                rows[ts_key][col] = qty
    return pd.DataFrame(rows.values())


def fetch_load(zone_code, zone_name, period_start, period_end):
    frames = []
    for process_type, col in [("A16", "realized_mw"), ("A01", "forecast_mw")]:
        resp = fetch({
            "documentType": "A65",
            "processType": process_type,
            "outBiddingZone_Domain": zone_code,
            "periodStart": period_start,
            "periodEnd": period_end,
        })
        df = parse_gl(resp, zone_name, col)
        logger.info(f"{zone_name} - load {col}: {len(df)} rows")
        frames.append(df)
    return frames[0].merge(frames[1], on=["timestamp", "zone"], how="outer")


def fetch_prices(zone_code, zone_name, period_start, period_end):
    resp = fetch({
        "documentType": "A44",
        "in_Domain": zone_code,
        "out_Domain": zone_code,
        "periodStart": period_start,
        "periodEnd": period_end,
    })
    df = parse_prices(resp, zone_name)
    logger.info(f"{zone_name} - prices: {len(df)} rows")
    return df


def fetch_generation(zone_code, zone_name, period_start, period_end):
    resp_actual = fetch({
        "documentType": "A75",
        "processType": "A16",
        "in_Domain": zone_code,
        "periodStart": period_start,
        "periodEnd": period_end,
    })
    df_actual = parse_generation(resp_actual, zone_name, forecast=False)
    logger.info(f"{zone_name} - generation actual: {len(df_actual)} rows")

    resp_forecast = fetch({
        "documentType": "A69",
        "processType": "A01",
        "in_Domain": zone_code,
        "periodStart": period_start,
        "periodEnd": period_end,
    })
    df_forecast = parse_generation(resp_forecast, zone_name, forecast=True)
    logger.info(f"{zone_name} - generation forecast: {len(df_forecast)} rows")

    if df_actual.empty:
        return df_forecast
    if df_forecast.empty:
        return df_actual
    return df_actual.merge(df_forecast, on=["timestamp", "zone"], how="outer")

_file_locks = {
    "load": threading.Lock(),
    "prices": threading.Lock(),
    "generation": threading.Lock(),
}


def append_parquet(df, dataset, year, month, zone_name):
    if df.empty:
        logger.warning(f"{zone_name} - {dataset}: no data available")
        return
    path = os.path.join(BASE_DIR, "data", dataset, f"year={year}", f"month={month:02d}", f"{dataset}.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with _file_locks[dataset]:
        if os.path.exists(path):
            existing = pd.read_parquet(path)
            existing = existing[existing["zone"] != zone_name]
            df = pd.concat([existing, df], ignore_index=True)
        df = df.drop_duplicates(subset=["timestamp", "zone"])
        df.to_parquet(path, index=False)
    logger.info(f"{zone_name} - {dataset}: written to disk")


def zone_already_processed(zone_name, year, month):
    for dataset in ["load", "prices", "generation"]:
        path = os.path.join(BASE_DIR, "data", dataset, f"year={year}", f"month={month:02d}", f"{dataset}.parquet")
        if not os.path.exists(path):
            return False
        try:
            with _file_locks[dataset]:
                existing = pd.read_parquet(path)
            if zone_name not in existing["zone"].values:
                return False
        except Exception:
            return False
    return True


def process_zone(zone_code, zone_name, period_start, period_end, year, month):
    if zone_already_processed(zone_name, year, month):
        logger.info(f"{zone_name} - already processed, skipping")
        return True
    try:
        logger.info(f"{zone_name} - starting")
        load_df = fetch_load(zone_code, zone_name, period_start, period_end)
        prices_df = fetch_prices(zone_code, zone_name, period_start, period_end)
        gen_df = fetch_generation(zone_code, zone_name, period_start, period_end)
        append_parquet(load_df, "load", year, month, zone_name)
        append_parquet(prices_df, "prices", year, month, zone_name)
        append_parquet(gen_df, "generation", year, month, zone_name)
        logger.info(f"{zone_name} - done")
        return True
    except Exception as e:
        import traceback
        logger.error(f"{zone_name} - failed: {traceback.format_exc()}")
        return False


def process_month(year, month, max_workers=15):
    period_start, period_end = month_boundaries(year, month)
    logger.info(f"Processing {year}-{month:02d} | period: {period_start} → {period_end} | workers: {max_workers}")

    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_zone, zone_code, zone_name, period_start, period_end, year, month): zone_name
            for zone_code, zone_name in ZONES.items()
        }
        for future in as_completed(futures):
            zone_name = futures[future]
            results[zone_name] = future.result()

    failed = [z for z, ok in results.items() if not ok]
    logger.info(f"{year}-{month:02d} done — {len(results) - len(failed)}/{len(results)} succeeded")
    return failed


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--start", type=str, help="YYYY-MM")
    parser.add_argument("--end", type=str, help="YYYY-MM")
    parser.add_argument("--workers", type=int, default=15)
    args = parser.parse_args()

    if args.backfill:
        start = datetime.strptime(args.start, "%Y-%m")
        end = datetime.strptime(args.end, "%Y-%m")

        months = []
        current = start
        while current <= end:
            months.append((current.year, current.month))
            month = current.month + 1 if current.month < 12 else 1
            year = current.year if current.month < 12 else current.year + 1
            current = datetime(year, month, 1)

        all_failures = {}
        backfill_start = time.time()

        with tqdm(total=len(months), unit="month", ncols=80) as pbar:
            for i, (year, month) in enumerate(months):
                month_label = f"{year}-{month:02d}"
                pbar.set_description(f"Processing {month_label}")

                t0 = time.time()
                failed = process_month(year, month, args.workers)
                elapsed = time.time() - t0

                if failed:
                    all_failures[month_label] = failed

                pbar.update(1)

                # estimate remaining time
                done = i + 1
                avg = (time.time() - backfill_start) / done
                remaining = avg * (len(months) - done)
                pbar.set_postfix({
                    "last": f"{elapsed:.0f}s",
                    "eta": f"{remaining/60:.1f}min"
                })

        print(f"\nBackfill complete in {(time.time() - backfill_start)/60:.1f} min")
        print(f"Log file: {log_file}")

        if all_failures:
            print("\nZones with missing data:")
            for month_key, zones in all_failures.items():
                print(f"  {month_key}: {zones}")
        else:
            print("All zones succeeded.")
    else:
        process_month(args.year, args.month, args.workers)