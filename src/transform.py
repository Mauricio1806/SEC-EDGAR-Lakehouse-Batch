"""
SILVER LAYER - Schema validation, filing metrics, anomaly detection
Inputs: bronze/sec_raw.csv + bronze/company_facts.json
"""

import os, json, logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TRANSFORM] %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR   = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"

RELEVANT_FORMS = ["10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "DEF 14A", "SC 13G", "SC 13G/A", "4", "S-1"]

SCHEMA = {
    "accessionNumber": str,
    "filingDate":      "date",
    "form":            str,
    "ticker":          str,
    "company_name":    str,
    "sector":          str,
}


def validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Running schema validation...")
    report = {}

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    for col, dtype in SCHEMA.items():
        col_lower = col.lower()
        if col_lower not in df.columns:
            logger.warning(f"  Missing column: {col_lower}")
            report[col_lower] = "MISSING"
            continue
        null_pct = df[col_lower].isnull().mean() * 100
        report[col_lower] = f"{null_pct:.1f}% nulls"
        if null_pct > 50:
            logger.warning(f"  High nulls in {col_lower}: {null_pct:.1f}%")

    logger.info(f"  Schema report: {report}")
    return df


def clean_filings(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Cleaning filings: {len(df)} rows input")

    df = df.dropna(how="all")

    for col in ["filingdate", "reportdate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "filingdate" in df.columns:
        df = df.dropna(subset=["filingdate"])

    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].fillna("N/A")

    dedup_cols = [c for c in ["accessionnumber", "ticker"] if c in df.columns]
    if dedup_cols:
        before = len(df)
        df = df.drop_duplicates(subset=dedup_cols)
        logger.info(f"  Dedup: {before} -> {len(df)} rows ({before - len(df)} removed)")

    if "form" in df.columns:
        before = len(df)
        df = df[df["form"].isin(RELEVANT_FORMS)]
        logger.info(f"  Form filter: {before} -> {len(df)} rows")

    df["transformed_at"] = datetime.utcnow().isoformat()
    logger.info(f"Clean output: {len(df)} rows")
    return df


def compute_filing_metrics(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing filing frequency metrics...")

    if "filingdate" not in df.columns or "ticker" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["filingdate"] = pd.to_datetime(df["filingdate"], errors="coerce")
    df = df.dropna(subset=["filingdate"])
    df["year"] = df["filingdate"].dt.year

    metrics = []
    for ticker, group in df.groupby("ticker"):
        group_sorted = group.sort_values("filingdate")
        total = len(group_sorted)
        annual_10k = len(group_sorted[group_sorted["form"] == "10-K"])
        quarterly_10q = len(group_sorted[group_sorted["form"] == "10-Q"])
        current_reports_8k = len(group_sorted[group_sorted["form"] == "8-K"])

        dates = group_sorted["filingdate"].sort_values()
        if len(dates) > 1:
            gaps = dates.diff().dropna().dt.days
            avg_gap = round(gaps.mean(), 1)
            max_gap = int(gaps.max())
            min_gap = int(gaps.min())
        else:
            avg_gap = max_gap = min_gap = 0

        years_active = group_sorted["year"].nunique()
        annual_rate = round(total / years_active, 1) if years_active > 0 else 0

        compliance_score = 100
        if annual_10k < years_active * 0.8:
            compliance_score -= 20
        if quarterly_10q < years_active * 2:
            compliance_score -= 15
        if max_gap > 365:
            compliance_score -= 15
        if avg_gap > 60:
            compliance_score -= 10
        compliance_score = max(0, compliance_score)

        metrics.append({
            "ticker":               ticker,
            "company_name":         group_sorted["company_name"].iloc[0],
            "sector":               group_sorted["sector"].iloc[0] if "sector" in group_sorted.columns else "N/A",
            "total_filings":        total,
            "annual_10k":           annual_10k,
            "quarterly_10q":        quarterly_10q,
            "current_reports_8k":   current_reports_8k,
            "avg_days_between_filings": avg_gap,
            "max_gap_days":         max_gap,
            "min_gap_days":         min_gap,
            "years_active":         years_active,
            "annual_filing_rate":   annual_rate,
            "compliance_score":     compliance_score,
            "first_filing":         str(dates.min().date()),
            "last_filing":          str(dates.max().date()),
        })

    result = pd.DataFrame(metrics).sort_values("compliance_score", ascending=False)
    logger.info(f"Filing metrics computed: {len(result)} companies")
    return result


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Detecting anomalies...")

    if "filingdate" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["filingdate"] = pd.to_datetime(df["filingdate"], errors="coerce")
    df = df.dropna(subset=["filingdate"])

    anomalies = []
    for ticker, group in df.groupby("ticker"):
        group_sorted = group.sort_values("filingdate")
        dates = group_sorted["filingdate"].sort_values()

        if len(dates) < 2:
            continue

        gaps = dates.diff().dropna().dt.days
        mean_gap = gaps.mean()
        std_gap  = gaps.std() if len(gaps) > 1 else 0
        threshold = mean_gap + (2.5 * std_gap)

        for i, (gap, date) in enumerate(zip(gaps, dates.iloc[1:])):
            if gap > threshold and gap > 180:
                anomalies.append({
                    "ticker":        ticker,
                    "anomaly_type":  "filing_gap",
                    "description":   f"Gap of {int(gap)} days between filings (threshold: {int(threshold)} days)",
                    "date_detected": str(date.date()),
                    "gap_days":      int(gap),
                    "severity":      "HIGH" if gap > 365 else "MEDIUM",
                })

        company_name = group_sorted["company_name"].iloc[0]
        sector       = group_sorted["sector"].iloc[0] if "sector" in group_sorted.columns else "N/A"
        years        = group_sorted["filingdate"].dt.year.unique()

        for year in years:
            year_filings = group_sorted[group_sorted["filingdate"].dt.year == year]
            k10_count = len(year_filings[year_filings["form"] == "10-K"])
            if year < datetime.now().year and k10_count == 0 and year >= 2010:
                anomalies.append({
                    "ticker":        ticker,
                    "anomaly_type":  "missing_annual_report",
                    "description":   f"No 10-K filing found for fiscal year {year}",
                    "date_detected": f"{year}-12-31",
                    "gap_days":      365,
                    "severity":      "HIGH",
                })

    result = pd.DataFrame(anomalies) if anomalies else pd.DataFrame(
        columns=["ticker","anomaly_type","description","date_detected","gap_days","severity"])
    logger.info(f"Anomalies detected: {len(result)}")
    return result


def build_annual_volume(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building annual filing volume by sector...")

    if "filingdate" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["filingdate"] = pd.to_datetime(df["filingdate"], errors="coerce")
    df = df.dropna(subset=["filingdate"])
    df["year"] = df["filingdate"].dt.year
    sector_col = "sector" if "sector" in df.columns else "ticker"

    result = (
        df[df["year"].between(2010, 2024)]
        .groupby(["year", sector_col, "form"])
        .agg(count=("form", "count"))
        .reset_index()
        .sort_values(["year", sector_col])
    )
    return result


def run_transform():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    input_path = BRONZE_DIR / "sec_raw.csv"
    if not input_path.exists():
        raise FileNotFoundError(f"Run extract first. Missing: {input_path}")

    df = pd.read_csv(input_path, low_memory=False)
    logger.info(f"Loaded bronze: {df.shape} — {df['ticker'].nunique() if 'ticker' in df.columns else '?'} companies")

    df = validate_schema(df)
    df_clean = clean_filings(df)
    df_clean.to_csv(SILVER_DIR / "sec_clean.csv", index=False)
    logger.info(f"Silver clean saved: {len(df_clean)} rows")

    metrics_df = compute_filing_metrics(df_clean)
    if not metrics_df.empty:
        metrics_df.to_csv(SILVER_DIR / "filing_metrics.csv", index=False)
        logger.info(f"Filing metrics saved: {len(metrics_df)} companies")

    anomalies_df = detect_anomalies(df_clean)
    anomalies_df.to_csv(SILVER_DIR / "anomalies.csv", index=False)
    logger.info(f"Anomalies saved: {len(anomalies_df)} records")

    annual_df = build_annual_volume(df_clean)
    if not annual_df.empty:
        annual_df.to_csv(SILVER_DIR / "annual_volume.csv", index=False)
        logger.info(f"Annual volume saved: {len(annual_df)} rows")

    for ticker in df_clean["ticker"].unique() if "ticker" in df_clean.columns else []:
        subset = df_clean[df_clean["ticker"] == ticker]
        subset.to_csv(SILVER_DIR / f"{ticker}_clean.csv", index=False)

    logger.info("Silver transformation complete.")
    return str(SILVER_DIR / "sec_clean.csv")


if __name__ == "__main__":
    run_transform()
