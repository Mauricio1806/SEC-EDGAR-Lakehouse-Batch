"""
SEC EDGAR Lakehouse - Pipeline orchestrator
Run: python src/main.py
"""

import logging, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from extract import run_extract
from transform import run_transform
from load import run_load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PIPELINE] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def run_pipeline():
    start = time.time()

    logger.info("=" * 60)
    logger.info("SEC EDGAR LAKEHOUSE PIPELINE — START")
    logger.info("50 companies · 3 endpoints · Medallion architecture")
    logger.info("=" * 60)

    logger.info("STEP 1/3 — EXTRACT (Bronze Layer)")
    logger.info("Estimated time: 3-5 minutes (rate limiting: 0.12s/request)")
    t0 = time.time()
    bronze = run_extract()
    logger.info(f"Extract complete in {time.time()-t0:.1f}s")

    logger.info("STEP 2/3 — TRANSFORM (Silver Layer)")
    t0 = time.time()
    run_transform()
    logger.info(f"Transform complete in {time.time()-t0:.1f}s")

    logger.info("STEP 3/3 — LOAD (Gold Layer)")
    t0 = time.time()
    run_load()
    logger.info(f"Load complete in {time.time()-t0:.1f}s")

    duration = time.time() - start
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE — {duration:.1f}s total")
    logger.info("Outputs: data/bronze/ · data/silver/ · data/gold/")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
