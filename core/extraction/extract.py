import argparse
import logging
import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from calendar import monthrange
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

API_TOKEN = os.getenv("API_TOKEN")
BASE_URL = "https://web-api.tp.entsoe.eu/api"

NS_GL   = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
NS_PUB  = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

ZONES = {
    "10Y1001A1001A82H": "DE-LU",
    "10YFR-RTE------C": "FR",
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
    start = f"{year}{month:02d}010000"
    last_day = monthrange(year, month)[1]
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    end = f"{next_year}{next_month:02d}010000"
    return start, end


def fetch(params):
    resp = requests.get(BASE_URL, params={"securityToken": API_TOKEN, **params})
    resp.raise_for_status()
    return resp


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
    logger.info(f"{zone_name} - fetching load (realized + forecast)")
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
        frames.append(df)
        logger.info(f"{zone_name} - load {col}: {len(df)} rows")
    merged = frames[0].merge(frames[1], on=["timestamp", "zone"], how="outer")
    return merged


def fetch_prices(zone_code, zone_name, period_start, period_end):
    logger.info(f"{zone_name} - fetching prices")
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
    logger.info(f"{zone_name} - fetching generation (actual + wind/solar forecast)")
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

    merged = df_actual.merge(df_forecast, on=["timestamp", "zone"], how="outer")
    return merged


def write_parquet(df, dataset, year, month):
    path = os.path.join(BASE_DIR, "data", dataset, f"year={year}", f"month={month:02d}", f"{dataset}.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info(f"Written {len(df)} rows to {path}")


def append_parquet(df, dataset, year, month):
    path = os.path.join(BASE_DIR, "data", dataset, f"year={year}", f"month={month:02d}", f"{dataset}.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        existing = pd.read_parquet(path)
        zone = df["zone"].iloc[0]
        existing = existing[existing["zone"] != zone]
        df = pd.concat([existing, df], ignore_index=True)
    df.to_parquet(path, index=False)
    logger.info(f"{dataset}: {len(df)} total rows written")
    

def process_month(year, month):
    period_start, period_end = month_boundaries(year, month)
    logger.info(f"Processing {year}-{month:02d} | period: {period_start} → {period_end}")

    for zone_code, zone_name in ZONES.items():
        try:
            load_df = fetch_load(zone_code, zone_name, period_start, period_end)
            prices_df = fetch_prices(zone_code, zone_name, period_start, period_end)
            gen_df = fetch_generation(zone_code, zone_name, period_start, period_end)

            append_parquet(load_df, "load", year, month)
            append_parquet(prices_df, "prices", year, month)
            append_parquet(gen_df, "generation", year, month)

            logger.info(f"{zone_name} - written to disk")
        except Exception as e:
            logger.error(f"{zone_name} - failed: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    process_month(args.year, args.month)