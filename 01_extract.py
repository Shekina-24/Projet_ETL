"""
Phase 1 — Extraction
Téléchargement, lecture et profilage des fichiers DVF bruts.
"""

import csv
import gzip
import io
import logging
import os
from pathlib import Path

import pandas as pd
import requests

os.makedirs("data/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data/logs/pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv.gz"
DATA_RAW = Path("data/raw")
OUTPUT_DIR = Path("output")


def download_dvf(year: int, dept: str) -> Path:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    dest = DATA_RAW / f"dvf_{year}_{dept}.csv"
    if dest.exists():
        logger.info(f"Fichier déjà présent : {dest}")
        return dest
    url = BASE_URL.format(year=year, dept=dept)
    logger.info(f"Téléchargement : {url}")
    response = requests.get(url, timeout=120, stream=True)
    response.raise_for_status()
    tmp_gz = dest.with_suffix(".csv.gz")
    with open(tmp_gz, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    with gzip.open(tmp_gz, "rb") as f_in, open(dest, "wb") as f_out:
        f_out.write(f_in.read())
    tmp_gz.unlink()
    logger.info(f"Fichier décompressé et sauvegardé : {dest} ({dest.stat().st_size / 1e6:.1f} Mo)")
    return dest


def _detect_separator(filepath: Path) -> str:
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        sample = f.read(4096)
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=",;")
        return dialect.delimiter
    except csv.Error:
        return ","


def load_raw_files(data_dir: str = "data/raw") -> pd.DataFrame:
    path = Path(data_dir)
    csv_files = sorted(path.glob("dvf_*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"Aucun fichier DVF trouvé dans {data_dir}")
    frames = []
    for fp in csv_files:
        sep = _detect_separator(fp)
        logger.info(f"Lecture de {fp.name} (séparateur='{sep}')")
        df = pd.read_csv(fp, sep=sep, encoding="utf-8", dtype=str, low_memory=False)
        df["_source_file"] = fp.name
        frames.append(df)
        logger.info(f"  → {len(df):,} lignes chargées")
    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"Total combiné : {len(combined):,} lignes, {len(combined.columns)} colonnes")
    return combined


def profile_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    records = []
    numeric_cols = {"valeur_fonciere", "surface_reelle_bati", "surface_terrain",
                    "nombre_lots", "nombre_pieces_principales",
                    "lot1_surface_carrez", "lot2_surface_carrez",
                    "lot3_surface_carrez", "lot4_surface_carrez", "lot5_surface_carrez",
                    "longitude", "latitude"}
    for col in df.columns:
        series = df[col]
        nb_missing = series.isna().sum()
        pct_missing = round(nb_missing / len(df) * 100, 2)
        detected_type = str(series.dtype)
        rec = {"colonne": col, "type_detecte": detected_type, "nb_manquants": nb_missing,
               "pct_manquants": pct_missing, "nb_uniques": series.nunique(),
               "min": None, "max": None, "moyenne": None, "top5_valeurs": None}
        if col in numeric_cols:
            s_num = pd.to_numeric(series, errors="coerce")
            rec["min"] = round(s_num.min(), 2) if not s_num.isna().all() else None
            rec["max"] = round(s_num.max(), 2) if not s_num.isna().all() else None
            rec["moyenne"] = round(s_num.mean(), 2) if not s_num.isna().all() else None
        else:
            top5 = series.value_counts().head(5)
            rec["top5_valeurs"] = " | ".join(f"{v}({c})" for v, c in top5.items())
        records.append(rec)
    profile_df = pd.DataFrame(records)
    logger.info(f"Profilage terminé : {len(profile_df)} colonnes analysées")
    return profile_df


def run_extraction(years, depts) -> pd.DataFrame:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for year in years:
        for dept in depts:
            download_dvf(year, dept)
    df_raw = load_raw_files()

    raw_combined_path = OUTPUT_DIR / "dvf_raw_combined.csv"
    df_raw.to_csv(raw_combined_path, index=False, encoding="utf-8")
    logger.info(f"Données brutes combinées exportées → {raw_combined_path} ({len(df_raw):,} lignes)")

    profile = profile_dataframe(df_raw)
    profile_path = OUTPUT_DIR / "profile_dvf.csv"
    profile.to_csv(profile_path, index=False, encoding="utf-8")
    logger.info(f"Profil exporté → {profile_path}")
    return df_raw


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Phase 1 — Extraction DVF")
    parser.add_argument("--year", nargs="+", type=int, default=[2022, 2023])
    parser.add_argument("--dept", nargs="+", default=["75"])
    args = parser.parse_args()
    df = run_extraction(args.year, args.dept)
    print(f"\nExtraction terminée : {len(df):,} lignes brutes chargées.")