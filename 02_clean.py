"""
Phase 2 — Nettoyage
Déduplication, typage, gestion des valeurs manquantes et aberrantes.
Produit dvf_clean.csv, rejets_clean.csv, stats_clean.json.
"""

import json
import logging
import os
from pathlib import Path

import pandas as pd

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

INPUT_PATH = Path("output/dvf_raw_combined.csv")
OUTPUT_CLEAN = Path("output/dvf_clean.csv")
OUTPUT_REJETS = Path("output/rejets_clean.csv")
OUTPUT_STATS = Path("output/stats_clean.json")


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Déduplication en cours…")
    df = df.copy()
    df["surface_reelle_bati_num"] = pd.to_numeric(
        df.get("surface_reelle_bati", pd.Series(dtype=float)), errors="coerce"
    )
    bien_principal = df[df["type_local"].isin(["Maison", "Appartement"])].copy()
    bien_principal = (
        bien_principal
        .sort_values("surface_reelle_bati_num", ascending=False)
        .drop_duplicates(subset="id_mutation", keep="first")
    )
    mutations_avec_bati = set(bien_principal["id_mutation"])
    reste = df[~df["id_mutation"].isin(mutations_avec_bati)].drop_duplicates(
        subset="id_mutation", keep="first"
    )
    result = pd.concat([bien_principal, reste], ignore_index=True)
    result = result.drop(columns=["surface_reelle_bati_num"], errors="ignore")
    nb_avant = df["id_mutation"].nunique()
    nb_apres = len(result)
    logger.info(f"Déduplication : {nb_avant:,} mutations → {nb_apres:,} lignes conservées")
    return result


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "valeur_fonciere" in df.columns:
        df["valeur_fonciere"] = (
            df["valeur_fonciere"]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
        )
    if "date_mutation" in df.columns:
        df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce")
    if "code_commune" in df.columns:
        df["code_commune"] = (
            df["code_commune"].astype(str).str.strip().str.zfill(5)
        )
    logger.info("Conversion des types effectuée")
    return df


def handle_missing(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    rejets = []
    if "type_local" in df.columns:
        df["type_local"] = df["type_local"].fillna("Inconnu")
    mask_vf_nulle = df["valeur_fonciere"].isna() | (df["valeur_fonciere"] == 0)
    rej = df[mask_vf_nulle].copy()
    rej["motif_rejet"] = "valeur_fonciere nulle ou absente"
    rejets.append(rej)
    df = df[~mask_vf_nulle]
    logger.info(f"Valeurs manquantes : {len(rej):,} lignes rejetées (valeur_fonciere nulle)")
    return df, pd.concat(rejets, ignore_index=True) if rejets else pd.DataFrame()


def filter_aberrations(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rejets = []
    mask_bas = df["valeur_fonciere"] < 1_000
    rej_bas = df[mask_bas].copy()
    rej_bas["motif_rejet"] = "valeur_fonciere < 1 000 € (transfert symbolique)"
    rejets.append(rej_bas)
    mask_haut = df["valeur_fonciere"] > 50_000_000
    rej_haut = df[~mask_bas & mask_haut].copy()
    rej_haut["motif_rejet"] = "valeur_fonciere > 50 000 000 € (outlier extrême)"
    rejets.append(rej_haut)
    df_valide = df[~mask_bas & ~mask_haut]
    nb_rej = len(rej_bas) + len(rej_haut)
    logger.info(f"Filtrage aberrations : {nb_rej:,} lignes rejetées")
    return df_valide, pd.concat(rejets, ignore_index=True)


def filter_nature(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    mask_vente = df["nature_mutation"] == "Vente"
    df_valide = df[mask_vente].copy()
    rej = df[~mask_vente].copy()
    rej["motif_rejet"] = f"nature_mutation != Vente ({rej['nature_mutation'].unique()[:5].tolist()})"
    logger.info(
        f"Filtrage nature : {len(df_valide):,} ventes conservées, {len(rej):,} rejetées"
    )
    return df_valide, rej


def run_cleaning(df_raw: pd.DataFrame | None = None) -> pd.DataFrame:
    Path("output").mkdir(parents=True, exist_ok=True)
    if df_raw is None:
        logger.info(f"Lecture de {INPUT_PATH}")
        df_raw = pd.read_csv(INPUT_PATH, dtype=str, low_memory=False, encoding="utf-8")

    nb_entree = len(df_raw)
    logger.info(f"Nettoyage — entrée : {nb_entree:,} lignes")

    all_rejets = []
    stats_motifs: dict[str, int] = {}

    df = deduplicate(df_raw)
    nb_apres_dedup = len(df)
    df = convert_types(df)

    df, rej_missing = handle_missing(df)
    if len(rej_missing):
        all_rejets.append(rej_missing)
        stats_motifs["valeur_fonciere nulle"] = len(rej_missing)

    df, rej_aberr = filter_aberrations(df)
    if len(rej_aberr):
        all_rejets.append(rej_aberr)
        for motif, grp in rej_aberr.groupby("motif_rejet"):
            stats_motifs[motif] = len(grp)

    df, rej_nature = filter_nature(df)
    if len(rej_nature):
        all_rejets.append(rej_nature)
        stats_motifs["nature_mutation != Vente"] = len(rej_nature)

    df.to_csv(OUTPUT_CLEAN, index=False, encoding="utf-8")
    logger.info(f"dvf_clean.csv → {len(df):,} lignes")

    if all_rejets:
        rejets_df = pd.concat(all_rejets, ignore_index=True)
        rejets_df.to_csv(OUTPUT_REJETS, index=False, encoding="utf-8")
        logger.info(f"rejets_clean.csv → {len(rejets_df):,} lignes")
    else:
        pd.DataFrame().to_csv(OUTPUT_REJETS, index=False)

    nb_sortie = len(df)
    nb_rejetes = sum(stats_motifs.values())

    stats = {
        "nb_lignes_brutes_entree": nb_entree,
        "nb_mutations_apres_dedup": nb_apres_dedup,
        "nb_lignes_conservees": nb_sortie,
        "nb_lignes_rejetees": nb_rejetes,
        "motifs": stats_motifs,
    }
    with open(OUTPUT_STATS, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    logger.info(f"stats_clean.json → {stats}")

    return df


if __name__ == "__main__":
    df_clean = run_cleaning()
    print(f"\nNettoyage terminé : {len(df_clean):,} mutations valides.")