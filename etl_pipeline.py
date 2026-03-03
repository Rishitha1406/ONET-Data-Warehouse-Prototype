"""
O*NET Data Warehouse ETL Pipeline

This script:
  1. Extracts raw O*NET-style occupation data (CSV seed files)
  2. Transforms and cleans the data
  3. Loads structured tables into SQLite (warehouse.db)
  4. Runs data validation checks
"""

import sqlite3
import pandas as pd
import os
import re
import logging
from datetime import datetim

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "warehouse.db")
DATA_DIR = os.path.join(os.path.dirname(__file__), "raw_data")

# EXTRACT
def extract(filename: str) -> pd.DataFrame:
    # Try each possible file extension in order of preference
    base = filename.replace(".csv", "")
    candidates = [
        (filename,            None),
        (base + ".xlsx",      None),
        (base + ".txt",       None),
    ]
    path = None
    sep  = ","
    ext  = ".csv"
    for fname, _ in candidates:
        candidate_path = os.path.join(DATA_DIR, fname)
        if os.path.exists(candidate_path):
            path = candidate_path
            ext  = os.path.splitext(fname)[1].lower()
            break
    if path is None:
        raise FileNotFoundError(
            f"Could not find {filename} (or .xlsx / .txt variant) in {DATA_DIR}"
        )
    log.info(f"Extracting {path}")
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, engine="openpyxl")
    else:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            first_line = f.readline()
        sep = "\t" if "\t" in first_line else ","
        df = pd.read_csv(path, sep=sep, encoding="utf-8", errors="replace")
    log.info(f"  → {len(df)} rows, {len(df.columns)} columns")
    return df


# TRANSFORM HELPERS

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase, strip whitespace, replace spaces/special chars with underscores."""
    df.columns = [
        re.sub(r"[^a-z0-9]+", "_", col.strip().lower()).strip("_")
        for col in df.columns
    ]
    # Normalize O*NET-specific column name variants
    rename_map = {
        "o_net_soc_code": "onetsoc_code",
        "onetsoc_code": "onetsoc_code",
        "element_id": "element_id",
        "element_name": "element_name",
        "scale_id": "scale_id",
        "data_value": "data_value",
        "task_id": "task_id",
        "task_type": "task_type",
        "incumbents_responding": "incumbents_responding",
        "annual_median_wage": "annual_median_wage",
        "annual_10th_pct": "annual_10th_pct",
        "annual_90th_pct": "annual_90th_pct",
        "hourly_median_wage": "hourly_median_wage",
        "wage_year": "wage_year",
        "date": "date_updated",
        "category": "category",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
    return df


def normalize_soc_code(series: pd.Series) -> pd.Series:
    """Ensure SOC codes follow XX-XXXX.XX format."""
    return series.str.strip().str.upper()


def drop_null_keys(df: pd.DataFrame, key_cols: list) -> pd.DataFrame:
    """Remove rows where any key column is null."""
    before = len(df)
    df = df.dropna(subset=key_cols)
    dropped = before - len(df)
    if dropped:
        log.warning(f"  Dropped {dropped} rows with null key columns {key_cols}")
    return df


def clip_scale(df: pd.DataFrame, col: str, lo: float, hi: float) -> pd.DataFrame:
    """Clip numeric values to a valid range."""
    df[col] = pd.to_numeric(df[col], errors="coerce").clip(lo, hi)
    return df


# TRANSFORM – each staging table

def transform_occupations(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_column_names(df)
    df = drop_null_keys(df, ["onetsoc_code", "title"])
    df["onetsoc_code"] = normalize_soc_code(df["onetsoc_code"])
    df["title"] = df["title"].str.strip().str.title()
    df["description"] = df["description"].fillna("").str.strip()
    # Derive broad SOC group (first 2 digits)
    df["soc_major_group"] = df["onetsoc_code"].str[:2]
    return df[["onetsoc_code", "title", "description", "soc_major_group"]]


def transform_skills(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_column_names(df)
    df = drop_null_keys(df, ["onetsoc_code", "element_id", "element_name"])
    df["onetsoc_code"] = normalize_soc_code(df["onetsoc_code"])
    df["scale_id"] = df["scale_id"].str.strip().str.upper()
    df = clip_scale(df, "data_value", 0, 7)
    df["data_value"] = df["data_value"].round(2)
    df["date_updated"] = pd.to_datetime(df.get("date_updated", datetime.now().date()), errors="coerce")
    return df[["onetsoc_code", "element_id", "element_name", "scale_id", "data_value", "date_updated"]]


def transform_knowledge(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_column_names(df)
    df = drop_null_keys(df, ["onetsoc_code", "element_id", "element_name"])
    df["onetsoc_code"] = normalize_soc_code(df["onetsoc_code"])
    df["scale_id"] = df["scale_id"].str.strip().str.upper()
    df = clip_scale(df, "data_value", 0, 7)
    df["data_value"] = df["data_value"].round(2)
    df["date_updated"] = pd.to_datetime(df.get("date_updated", datetime.now().date()), errors="coerce")
    return df[["onetsoc_code", "element_id", "element_name", "scale_id", "data_value", "date_updated"]]


def transform_education(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_column_names(df)
    df = drop_null_keys(df, ["onetsoc_code", "element_id"])
    df["onetsoc_code"] = normalize_soc_code(df["onetsoc_code"])
    df["scale_id"] = df["scale_id"].str.strip().str.upper()
    df = clip_scale(df, "data_value", 0, 100)
    df["data_value"] = df["data_value"].round(2)
    df["category"] = pd.to_numeric(df.get("category", 0), errors="coerce").fillna(0).astype(int)
    return df[["onetsoc_code", "element_id", "element_name", "scale_id", "category", "data_value"]]


def transform_tasks(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_column_names(df)
    df = drop_null_keys(df, ["onetsoc_code", "task_id", "task"])
    df["onetsoc_code"] = normalize_soc_code(df["onetsoc_code"])
    df["task"] = df["task"].str.strip()
    df["task_type"] = df.get("task_type", pd.Series(["Core"] * len(df))).fillna("Core")
    df["incumbents_responding"] = pd.to_numeric(df.get("incumbents_responding", 0), errors="coerce").fillna(0).astype(int)
    return df[["onetsoc_code", "task_id", "task", "task_type", "incumbents_responding"]]


def transform_wages(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_column_names(df)

    # Map real BLS column names to warehouse column names
    bls_rename = {
        "occ_code":   "onetsoc_code",
        "a_median":   "annual_median_wage",
        "a_pct10":    "annual_10th_pct",
        "a_pct90":    "annual_90th_pct",
        "h_median":   "hourly_median_wage",
    }
    df.rename(columns={k: v for k, v in bls_rename.items() if k in df.columns}, inplace=True)

    # Keep national-level data only
    if "area_type" in df.columns:
        df = df[df["area_type"] == 1]

    df = drop_null_keys(df, ["onetsoc_code"])
    df["onetsoc_code"] = normalize_soc_code(df["onetsoc_code"])

    # Keep only detailed occupations - last 2 digits must not be 00
    df = df[df["onetsoc_code"].str[-2:] != "00"]

    # Add .00 suffix to match O*NET format
    df["onetsoc_code"] = df["onetsoc_code"] + ".00"

    for col in ["annual_median_wage", "annual_10th_pct", "annual_90th_pct", "hourly_median_wage"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["wage_year"] = 2023

    return df[["onetsoc_code", "annual_median_wage", "annual_10th_pct", "annual_90th_pct", "hourly_median_wage", "wage_year"]]


# LOAD

DDL = """
-- Dimension: Occupation
CREATE TABLE IF NOT EXISTS dim_occupation (
    onetsoc_code   TEXT PRIMARY KEY,
    title          TEXT NOT NULL,
    description    TEXT,
    soc_major_group TEXT
);

-- Dimension: Skill Element
CREATE TABLE IF NOT EXISTS dim_skill_element (
    element_id   TEXT PRIMARY KEY,
    element_name TEXT NOT NULL
);

-- Fact: Skills
CREATE TABLE IF NOT EXISTS fact_skills (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    onetsoc_code TEXT NOT NULL REFERENCES dim_occupation(onetsoc_code),
    element_id   TEXT NOT NULL,
    element_name TEXT,
    scale_id     TEXT,
    data_value   REAL,
    date_updated TEXT
);

-- Fact: Knowledge
CREATE TABLE IF NOT EXISTS fact_knowledge (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    onetsoc_code TEXT NOT NULL REFERENCES dim_occupation(onetsoc_code),
    element_id   TEXT NOT NULL,
    element_name TEXT,
    scale_id     TEXT,
    data_value   REAL,
    date_updated TEXT
);

-- Fact: Education Requirements
CREATE TABLE IF NOT EXISTS fact_education (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    onetsoc_code TEXT NOT NULL REFERENCES dim_occupation(onetsoc_code),
    element_id   TEXT,
    element_name TEXT,
    scale_id     TEXT,
    category     INTEGER,
    data_value   REAL
);

-- Fact: Tasks
CREATE TABLE IF NOT EXISTS fact_tasks (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    onetsoc_code          TEXT NOT NULL REFERENCES dim_occupation(onetsoc_code),
    task_id               INTEGER,
    task                  TEXT,
    task_type             TEXT,
    incumbents_responding INTEGER
);

-- Fact: Wages
CREATE TABLE IF NOT EXISTS fact_wages (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    onetsoc_code        TEXT NOT NULL REFERENCES dim_occupation(onetsoc_code),
    annual_median_wage  REAL,
    annual_10th_pct     REAL,
    annual_90th_pct     REAL,
    hourly_median_wage  REAL,
    wage_year           INTEGER
);
"""


def create_schema(conn: sqlite3.Connection):
    log.info("Creating schema…")
    conn.executescript(DDL)
    conn.commit()


def load_table(conn: sqlite3.Connection, df: pd.DataFrame, table: str, if_exists="replace"):
    log.info(f"Loading {len(df)} rows → {table}")
    df.to_sql(table, conn, if_exists=if_exists, index=False)


# VALIDATION

def run_validation(conn: sqlite3.Connection):
    log.info("── Running data validation ──")
    checks = {
        "No null SOC codes in dim_occupation":
            "SELECT COUNT(*) FROM dim_occupation WHERE onetsoc_code IS NULL",
        "No null titles in dim_occupation":
            "SELECT COUNT(*) FROM dim_occupation WHERE title IS NULL OR title = ''",
        "Skills data_value in [0,7]":
            "SELECT COUNT(*) FROM fact_skills WHERE data_value < 0 OR data_value > 7",
        "Knowledge data_value in [0,7]":
            "SELECT COUNT(*) FROM fact_knowledge WHERE data_value < 0 OR data_value > 7",
        "Wages non-negative":
            "SELECT COUNT(*) FROM fact_wages WHERE annual_median_wage < 0",
        "Orphaned skill records (no matching occupation)":
            "SELECT COUNT(*) FROM fact_skills s LEFT JOIN dim_occupation o ON s.onetsoc_code=o.onetsoc_code WHERE o.onetsoc_code IS NULL",
    }
    results = {}
    all_pass = True
    for name, sql in checks.items():
        val = conn.execute(sql).fetchone()[0]
        status = "PASS" if val == 0 else "FAIL"
        if val != 0:
            all_pass = False
        log.info(f"  {status} | {name} (violations: {val})")
        results[name] = {"violations": val, "passed": val == 0}
    log.info("All checks passed" if all_pass else "Some checks FAILED")
    return results


# MAIN

def run_etl():
    log.info("=== O*NET Data Warehouse ETL Pipeline ===")
    conn = sqlite3.connect(DB_PATH)
    create_schema(conn)

    steps = [
        ("occupation_data.csv", transform_occupations, "dim_occupation"),
        ("skills.csv",          transform_skills,      "fact_skills"),
        ("knowledge.csv",       transform_knowledge,   "fact_knowledge"),
        ("education_training_experience.csv", transform_education, "fact_education"),
        ("task_statements.csv", transform_tasks,       "fact_tasks"),
        ("wages.csv",           transform_wages,       "fact_wages"),
    ]

    for filename, transform_fn, table in steps:
        raw = extract(filename)
        clean = transform_fn(raw)
        load_table(conn, clean, table)

    validation_results = run_validation(conn)
    conn.close()
    log.info(f"ETL complete. Database written to: {DB_PATH}")
    return validation_results


if __name__ == "__main__":
    run_etl()
