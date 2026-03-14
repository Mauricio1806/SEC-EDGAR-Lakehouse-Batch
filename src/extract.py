"""
BRONZE LAYER - SEC EDGAR Multi-Endpoint Extractor
Coleta: company_tickers + submissions + companyfacts
50 empresas S&P 500 com rate limiting automatico
"""

import os, json, time, logging
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [EXTRACT] %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "SEC-EDGAR-Lakehouse-Portfolio mauri@portfolio.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov"
}

BASE_DIR   = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"

SP500_TOP50 = {
    "AAPL":  "0000320193", "MSFT": "0000789019", "AMZN": "0001018724",
    "NVDA":  "0001045810", "GOOGL":"0001652044", "META": "0001326801",
    "BRK-B": "0001067983", "LLY":  "0000059478", "JPM":  "0000019617",
    "AVGO":  "0001730168", "XOM":  "0000034088", "TSLA": "0001318605",
    "UNH":   "0000731766", "V":    "0001403161", "PG":   "0000080424",
    "MA":    "0001141391", "JNJ":  "0000200406", "HD":   "0000354950",
    "MRK":   "0000310158", "COST": "0000909832", "ABBV": "0001551152",
    "CVX":   "0000093410", "CRM":  "0001108524", "BAC":  "0000070858",
    "NFLX":  "0001065280", "AMD":  "0000002488", "KO":   "0000021344",
    "WMT":   "0000104169", "PEP":  "0000077476", "TMO":  "0000097476",
    "MCD":   "0000063908", "CSCO": "0000858877", "ABT":  "0000001800",
    "ACN":   "0001467373", "GE":   "0000040533", "DHR":  "0000313616",
    "TXN":   "0000097476", "ORCL": "0001341439", "NKE":  "0000320187",
    "PM":    "0001413329", "UNP":  "0000100885", "RTX":  "0000101829",
    "AMGN":  "0000820081", "IBM":  "0000051143", "CAT":  "0000018230",
    "SPGI":  "0000064040", "GS":   "0000886982", "BLK":  "0001364742",
    "SYK":   "0000310764", "NOW":  "0001373715"
}

SECTOR_MAP = {
    "AAPL":"Technology","MSFT":"Technology","NVDA":"Technology","GOOGL":"Technology",
    "META":"Technology","AVGO":"Technology","AMD":"Technology","CRM":"Technology",
    "CSCO":"Technology","TXN":"Technology","ORCL":"Technology","IBM":"Technology","NOW":"Technology",
    "AMZN":"Consumer Discretionary","TSLA":"Consumer Discretionary","HD":"Consumer Discretionary",
    "MCD":"Consumer Discretionary","NKE":"Consumer Discretionary","COST":"Consumer Discretionary",
    "WMT":"Consumer Staples","PG":"Consumer Staples","KO":"Consumer Staples","PEP":"Consumer Staples","PM":"Consumer Staples",
    "JPM":"Financials","BAC":"Financials","V":"Financials","MA":"Financials","GS":"Financials","BLK":"Financials","SPGI":"Financials",
    "XOM":"Energy","CVX":"Energy",
    "UNH":"Healthcare","LLY":"Healthcare","JNJ":"Healthcare","MRK":"Healthcare",
    "ABBV":"Healthcare","ABT":"Healthcare","TMO":"Healthcare","DHR":"Healthcare","AMGN":"Healthcare",
    "BRK-B":"Financials","ACN":"Technology","GE":"Industrials","RTX":"Industrials",
    "UNP":"Industrials","CAT":"Industrials","SYK":"Healthcare","NFLX":"Communication Services"
}


def rate_limited_get(url: str, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            time.sleep(0.12)
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 429:
                wait = 60
                logger.warning(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt+1}/{retries} failed: {e}")
            time.sleep(5 * (attempt + 1))
    return {}


def extract_submissions(ticker: str, cik: str) -> pd.DataFrame:
    cik_padded = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    data = rate_limited_get(url)
    if not data:
        return pd.DataFrame()

    raw_path = BRONZE_DIR / "submissions" / f"{ticker}_submissions.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    with open(raw_path, "w") as f:
        json.dump(data, f, indent=2)

    recent = data.get("filings", {}).get("recent", {})
    if not recent or not recent.get("accessionNumber"):
        return pd.DataFrame()

    df = pd.DataFrame(recent)
    df["ticker"]          = ticker
    df["cik"]             = data.get("cik", "")
    df["company_name"]    = data.get("name", "")
    df["sic"]             = data.get("sic", "")
    df["sic_description"] = data.get("sicDescription", "")
    df["sector"]          = SECTOR_MAP.get(ticker, "Other")
    df["fiscal_year_end"] = data.get("fiscalYearEnd", "")
    df["state_of_inc"]    = data.get("stateOfIncorporation", "")
    df["extracted_at"]    = datetime.utcnow().isoformat()
    return df


def extract_company_facts(ticker: str, cik: str) -> dict:
    cik_padded = cik.zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    data = rate_limited_get(url)
    if not data:
        return {}

    raw_path = BRONZE_DIR / "facts" / f"{ticker}_facts.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    with open(raw_path, "w") as f:
        json.dump(data, f, indent=2)

    facts = data.get("facts", {})
    us_gaap = facts.get("us-gaap", {})

    result = {"ticker": ticker, "cik": cik}

    for metric, keys in {
        "Revenues":          ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
        "NetIncomeLoss":     ["NetIncomeLoss"],
        "Assets":            ["Assets"],
        "StockholdersEquity":["StockholdersEquity", "StockholdersEquityAttributableToParent"],
        "OperatingIncomeLoss":["OperatingIncomeLoss"],
        "EarningsPerShare":  ["EarningsPerShareBasic"]
    }.items():
        for key in keys:
            if key in us_gaap:
                units = us_gaap[key].get("units", {})
                usd_data = units.get("USD") or units.get("shares") or []
                annual = [
                    x for x in usd_data
                    if x.get("form") in ("10-K", "10-K/A")
                    and x.get("fp") == "FY"
                    and len(str(x.get("end", ""))) == 10
                ]
                if annual:
                    annual_sorted = sorted(annual, key=lambda x: x["end"])
                    result[metric] = annual_sorted
                    break

    return result


def run_extract():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    (BRONZE_DIR / "submissions").mkdir(exist_ok=True)
    (BRONZE_DIR / "facts").mkdir(exist_ok=True)

    all_submissions = []
    all_facts = []
    errors = []

    total = len(SP500_TOP50)
    for i, (ticker, cik) in enumerate(SP500_TOP50.items(), 1):
        logger.info(f"[{i}/{total}] Processing {ticker} (CIK: {cik})")

        df = extract_submissions(ticker, cik)
        if not df.empty:
            all_submissions.append(df)
            logger.info(f"  Submissions: {len(df)} filings")
        else:
            errors.append({"ticker": ticker, "stage": "submissions"})
            logger.warning(f"  No submissions for {ticker}")

        facts = extract_company_facts(ticker, cik)
        if facts and len(facts) > 2:
            all_facts.append(facts)
            metrics = [k for k in facts if k not in ("ticker", "cik")]
            logger.info(f"  Facts: {len(metrics)} metrics — {metrics}")
        else:
            errors.append({"ticker": ticker, "stage": "facts"})
            logger.warning(f"  No facts for {ticker}")

    if all_submissions:
        combined = pd.concat(all_submissions, ignore_index=True)
        out = BRONZE_DIR / "sec_raw.csv"
        combined.to_csv(out, index=False)
        logger.info(f"Bronze submissions: {out} ({len(combined)} rows, {combined['ticker'].nunique()} companies)")

    if all_facts:
        facts_path = BRONZE_DIR / "company_facts.json"
        with open(facts_path, "w") as f:
            json.dump(all_facts, f, indent=2)
        logger.info(f"Bronze facts: {facts_path} ({len(all_facts)} companies)")

    if errors:
        err_df = pd.DataFrame(errors)
        err_df.to_csv(BRONZE_DIR / "extraction_errors.csv", index=False)
        logger.warning(f"Errors logged: {len(errors)}")

    logger.info("Bronze extraction complete.")
    return str(BRONZE_DIR / "sec_raw.csv")


if __name__ == "__main__":
    run_extract()
