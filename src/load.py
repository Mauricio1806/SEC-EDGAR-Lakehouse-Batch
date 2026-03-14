"""
GOLD LAYER - Financial metrics, sector analysis, compliance ranking
Input: silver/ + bronze/company_facts.json
"""

import json, logging
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [LOAD] %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR   = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR   = BASE_DIR / "data" / "gold"


def build_financial_metrics(facts_path: Path) -> pd.DataFrame:
    logger.info("Building financial metrics from company facts...")

    if not facts_path.exists():
        logger.warning("company_facts.json not found — skipping financial metrics")
        return pd.DataFrame()

    with open(facts_path) as f:
        all_facts = json.load(f)

    records = []
    for company in all_facts:
        ticker = company.get("ticker", "")
        cik    = company.get("cik", "")

        for metric in ["Revenues", "NetIncomeLoss", "Assets", "StockholdersEquity", "OperatingIncomeLoss"]:
            entries = company.get(metric, [])
            if not entries:
                continue
            for entry in entries:
                try:
                    records.append({
                        "ticker":     ticker,
                        "cik":        cik,
                        "metric":     metric,
                        "value":      float(entry.get("val", 0)),
                        "period_end": entry.get("end", ""),
                        "year":       int(entry.get("end", "0000")[:4]),
                        "form":       entry.get("form", ""),
                        "filed":      entry.get("filed", ""),
                    })
                except (ValueError, TypeError):
                    continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = df[df["year"].between(2010, 2024)]
    df = df.drop_duplicates(subset=["ticker", "metric", "year"])
    df["value_billions"] = (df["value"] / 1e9).round(3)

    logger.info(f"Financial metrics: {len(df)} records, {df['ticker'].nunique()} companies")
    return df


def build_revenue_growth(fin_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing revenue growth YoY...")

    if fin_df.empty or "metric" not in fin_df.columns:
        return pd.DataFrame()

    rev = fin_df[fin_df["metric"] == "Revenues"].copy()
    if rev.empty:
        rev = fin_df[fin_df["metric"].str.contains("Revenue", na=False)].copy()

    if rev.empty:
        return pd.DataFrame()

    rev = rev.sort_values(["ticker", "year"])
    rev["revenue_prev"] = rev.groupby("ticker")["value"].shift(1)
    rev["yoy_growth_pct"] = ((rev["value"] - rev["revenue_prev"]) / rev["revenue_prev"].abs() * 100).round(2)
    rev = rev.dropna(subset=["yoy_growth_pct"])
    rev = rev[rev["yoy_growth_pct"].between(-100, 500)]

    logger.info(f"Revenue growth records: {len(rev)}")
    return rev[["ticker","year","value","value_billions","revenue_prev","yoy_growth_pct","filed"]]


def build_sector_summary(clean_path: Path, metrics_path: Path) -> pd.DataFrame:
    logger.info("Building sector summary...")

    if not clean_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(clean_path, low_memory=False)
    if "sector" not in df.columns:
        return pd.DataFrame()

    df["filingdate"] = pd.to_datetime(df["filingdate"] if "filingdate" in df.columns else df.get("filingDate",""), errors="coerce")
    df = df.dropna(subset=["filingdate"])
    df["year"] = df["filingdate"].dt.year

    sector_stats = (
        df.groupby("sector")
        .agg(
            total_filings=("form", "count"),
            unique_companies=("ticker", "nunique"),
            unique_forms=("form", "nunique"),
            years_covered=("year", "nunique"),
            most_common_form=("form", lambda x: x.value_counts().index[0]),
        )
        .reset_index()
        .sort_values("total_filings", ascending=False)
    )

    if metrics_path.exists():
        met = pd.read_csv(metrics_path)
        if "sector" in met.columns and "compliance_score" in met.columns:
            avg_compliance = met.groupby("sector")["compliance_score"].mean().round(1).reset_index()
            avg_compliance.columns = ["sector", "avg_compliance_score"]
            sector_stats = sector_stats.merge(avg_compliance, on="sector", how="left")

    sector_stats["generated_at"] = datetime.utcnow().isoformat()
    logger.info(f"Sector summary: {len(sector_stats)} sectors")
    return sector_stats


def build_compliance_ranking(metrics_path: Path) -> pd.DataFrame:
    logger.info("Building compliance ranking...")

    if not metrics_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(metrics_path)
    required = ["ticker","company_name","sector","compliance_score","total_filings","annual_10k","avg_days_between_filings"]
    available = [c for c in required if c in df.columns]
    df = df[available].copy()

    if "compliance_score" in df.columns:
        df["compliance_tier"] = pd.cut(
            df["compliance_score"],
            bins=[0, 60, 75, 90, 100],
            labels=["NEEDS REVIEW", "ACCEPTABLE", "GOOD", "EXCELLENT"],
            include_lowest=True
        )
        df = df.sort_values("compliance_score", ascending=False)

    df["rank"] = range(1, len(df) + 1)
    df["generated_at"] = datetime.utcnow().isoformat()
    logger.info(f"Compliance ranking: {len(df)} companies")
    return df


def build_anomaly_summary(anomalies_path: Path) -> pd.DataFrame:
    logger.info("Building anomaly summary...")

    if not anomalies_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(anomalies_path)
    if df.empty:
        return df

    summary = (
        df.groupby(["ticker", "anomaly_type", "severity"])
        .agg(
            count=("anomaly_type", "count"),
            max_gap=("gap_days", "max"),
            latest_date=("date_detected", "max")
        )
        .reset_index()
        .sort_values(["severity", "count"], ascending=[True, False])
    )
    summary["generated_at"] = datetime.utcnow().isoformat()
    logger.info(f"Anomaly summary: {len(summary)} records")
    return summary


def run_load():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    facts_path    = BRONZE_DIR / "company_facts.json"
    clean_path    = SILVER_DIR / "sec_clean.csv"
    metrics_path  = SILVER_DIR / "filing_metrics.csv"
    anomalies_path = SILVER_DIR / "anomalies.csv"

    outputs = {}

    fin_df = build_financial_metrics(facts_path)
    if not fin_df.empty:
        fin_df.to_csv(GOLD_DIR / "financial_metrics.csv", index=False)
        outputs["financial_metrics"] = len(fin_df)

        rev_growth = build_revenue_growth(fin_df)
        if not rev_growth.empty:
            rev_growth.to_csv(GOLD_DIR / "revenue_growth.csv", index=False)
            outputs["revenue_growth"] = len(rev_growth)

    sector_df = build_sector_summary(clean_path, metrics_path)
    if not sector_df.empty:
        sector_df.to_csv(GOLD_DIR / "sector_summary.csv", index=False)
        outputs["sector_summary"] = len(sector_df)

    compliance_df = build_compliance_ranking(metrics_path)
    if not compliance_df.empty:
        compliance_df.to_csv(GOLD_DIR / "compliance_ranking.csv", index=False)
        outputs["compliance_ranking"] = len(compliance_df)

    anomaly_df = build_anomaly_summary(anomalies_path)
    if not anomaly_df.empty:
        anomaly_df.to_csv(GOLD_DIR / "anomaly_summary.csv", index=False)
        outputs["anomaly_summary"] = len(anomaly_df)

    meta = {
        "pipeline_run":  datetime.utcnow().isoformat(),
        "outputs":       outputs,
        "companies":     50,
        "layers":        ["bronze", "silver", "gold"],
        "storage":       "AWS S3 free tier",
    }
    with open(GOLD_DIR / "pipeline_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    logger.info(f"Gold layer complete: {outputs}")
    return str(GOLD_DIR)


if __name__ == "__main__":
    run_load()
