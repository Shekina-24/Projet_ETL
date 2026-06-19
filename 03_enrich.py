"""
Phase 3 — Enrichissement
Jointure géographique INSEE, calcul du prix au m², catégories de prix,
indicateurs temporels et grand appartement.
"""

import logging
import sys
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = Path(__file__).parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "output"
DATA_DIR = Path(__file__).parent / "data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# URL du référentiel communes INSEE (CSV)
COMMUNES_URL = "https://www.data.gouv.fr/fr/datasets/r/dbe8a621-a9c4-4bc3-9cae-be1699c5ff25"
COMMUNES_CACHE = DATA_DIR / "raw" / "communes_insee.csv"


# ---------------------------------------------------------------------------
# Chargement du référentiel communes
# ---------------------------------------------------------------------------
def load_communes_ref() -> pd.DataFrame:
    """
    Charge le référentiel communes/départements/régions depuis data.gouv.fr
    ou depuis le cache local si déjà téléchargé.
    """
    if COMMUNES_CACHE.exists():
        logger.info(f"[communes] Chargement depuis le cache : {COMMUNES_CACHE}")
    else:
        logger.info(f"[communes] Téléchargement du référentiel INSEE : {COMMUNES_URL}")
        try:
            r = requests.get(COMMUNES_URL, timeout=60)
            r.raise_for_status()
            COMMUNES_CACHE.write_bytes(r.content)
            logger.info(f"[communes] Référentiel sauvegardé : {COMMUNES_CACHE}")
        except Exception as e:
            logger.error(f"[communes] Échec du téléchargement : {e}")
            raise

    for sep in [",", ";"]:
        for enc in ["utf-8", "latin-1"]:
            try:
                df = pd.read_csv(COMMUNES_CACHE, sep=sep, encoding=enc, dtype=str, low_memory=False)
                if len(df.columns) > 3:
                    logger.info(f"[communes] Chargé : {len(df):,} communes (sep='{sep}', enc={enc})")
                    logger.info(f"[communes] Colonnes disponibles : {list(df.columns)}")
                    return df
            except Exception:
                continue

    raise ValueError("Impossible de lire le référentiel communes INSEE.")


def normalize_communes(df_ref: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise le référentiel pour avoir les colonnes attendues :
    code_commune, nom_commune, code_departement, nom_departement, code_region, nom_region

    ATTENTION (piège vérifié) : dans le fichier "Communes de France - Base
    des codes postaux" de data.gouv.fr, la colonne nommée "code_commune"
    n'est PAS le code INSEE complet -- c'est uniquement le numéro de la
    commune AU SEIN de son département (ex: "1" pour la 1ère commune de
    l'Ain, "123" pour Lyon dans le 69...). Cette valeur se répète dans
    presque tous les départements (d'où un nombre de valeurs uniques
    anormalement bas après dédoublonnage, ~900 au lieu de ~35 000 communes),
    et ne correspond donc à RIEN dans le code_commune à 5 chiffres du DVF.

    Le vrai code INSEE complet se trouve dans la colonne "code_commune_INSEE"
    (avec le zéro initial du département parfois perdu, ex: "1001" pour la
    commune 01001 -- restauré ici avec zfill(5)).

    On supprime donc explicitement la colonne "code_commune" d'origine avant
    de construire la bonne, pour éviter tout risque de collision de noms.
    """
    df_ref = df_ref.copy()

    if "code_commune" in df_ref.columns:
        df_ref = df_ref.drop(columns=["code_commune"])

    col_map = {}
    cols_lower = {c.lower(): c for c in df_ref.columns}

    candidates = {
        "code_commune": ["code_commune_insee", "code_insee", "insee_com", "codecommune", "code_com"],
        "nom_commune": ["nom_commune", "nom_com", "libelle_commune", "libelle", "nom"],
        "code_departement": ["code_departement", "dep", "code_dep", "departement"],
        "nom_departement": ["nom_departement", "libelle_departement", "nom_dep"],
        "code_region": ["code_region", "reg", "code_reg", "region"],
        "nom_region": ["nom_region", "libelle_region", "nom_reg"],
    }

    for target, options in candidates.items():
        for opt in options:
            if opt in cols_lower:
                col_map[cols_lower[opt]] = target
                break

    df_ref = df_ref.rename(columns=col_map)

    if "code_commune" in df_ref.columns:
        df_ref["code_commune"] = df_ref["code_commune"].astype(str).str.strip().str.zfill(5)

    keep = [c for c in ["code_commune", "nom_commune", "code_departement",
                         "nom_departement", "code_region", "nom_region"] if c in df_ref.columns]
    df_ref = df_ref[keep].drop_duplicates(subset=["code_commune"])

    logger.info(f"[communes] Référentiel normalisé : {len(df_ref):,} communes, colonnes : {keep}")
    return df_ref


# ---------------------------------------------------------------------------
# Enrichissements
# ---------------------------------------------------------------------------
def join_geographie(df: pd.DataFrame, df_ref: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Jointure sur code_commune. Retourne (df_enrichi, df_rejets_geo)."""
    logger.info("[geo] Jointure géographique…")
    df_merged = df.merge(df_ref, on="code_commune", how="left", suffixes=("", "_ref"))

    mask_non_trouve = df_merged["nom_departement"].isna() if "nom_departement" in df_merged.columns else df_merged["code_region"].isna() if "code_region" in df_merged.columns else pd.Series(False, index=df_merged.index)

    df_rejets = df_merged[mask_non_trouve].copy()
    df_rejets["motif_rejet"] = "commune non trouvée dans référentiel INSEE"
    df_ok = df_merged[~mask_non_trouve].copy()

    taux = len(df_ok) / len(df) * 100 if len(df) > 0 else 0
    logger.info(f"[geo] Taux de jointure : {taux:.1f}% ({len(df_ok):,}/{len(df):,})")
    if taux < 95:
        logger.warning(f"[geo] Taux de jointure < 95% — vérifier le référentiel INSEE")

    return df_ok, df_rejets


def calcul_prix_m2(df: pd.DataFrame) -> pd.DataFrame:
    """prix_m2 = valeur_fonciere / surface_reelle_bati (NaN si surface absente ou nulle)."""
    df = df.copy()
    surface = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")
    prix = pd.to_numeric(df["valeur_fonciere"], errors="coerce")
    df["prix_m2"] = (prix / surface).where(surface > 0)
    nb_calcule = df["prix_m2"].notna().sum()
    logger.info(f"[prix_m2] Calculé pour {nb_calcule:,} mutations")
    return df


def calcul_categorie_prix(df: pd.DataFrame) -> pd.DataFrame:
    """Catégorie selon la valeur foncière."""
    def categoriser(v):
        if pd.isna(v):
            return None
        if v < 100_000:
            return "bas"
        elif v < 300_000:
            return "moyen"
        elif v < 800_000:
            return "eleve"
        else:
            return "premium"

    df = df.copy()
    df["categorie_prix"] = df["valeur_fonciere"].apply(categoriser)
    logger.info(f"[categorie_prix] Distribution : {df['categorie_prix'].value_counts().to_dict()}")
    return df


def decomposition_temporelle(df: pd.DataFrame) -> pd.DataFrame:
    """Extrait annee, mois, trimestre, semestre depuis date_mutation."""
    df = df.copy()
    date = pd.to_datetime(df["date_mutation"], errors="coerce")
    df["annee"] = date.dt.year
    df["mois"] = date.dt.month
    df["trimestre"] = date.dt.quarter
    df["semestre"] = (date.dt.month > 6).astype(int) + 1
    logger.info("[temps] Colonnes temporelles ajoutées : annee, mois, trimestre, semestre")
    return df


def calcul_grand_appart(df: pd.DataFrame) -> pd.DataFrame:
    """IS_GRAND_APPART = 1 si Appartement et nb_pieces >= 4."""
    df = df.copy()
    nb_pieces = pd.to_numeric(df["nombre_pieces_principales"], errors="coerce")
    df["IS_GRAND_APPART"] = (
        (df["type_local"] == "Appartement") & (nb_pieces >= 4)
    ).astype(int)
    nb = df["IS_GRAND_APPART"].sum()
    logger.info(f"[grand_appart] {nb:,} grands appartements (>= 4 pièces)")
    return df


# ---------------------------------------------------------------------------
# Exécution principale
# ---------------------------------------------------------------------------
def run_enrich(df_clean: pd.DataFrame = None) -> pd.DataFrame:
    """
    Orchestre l'enrichissement complet.
    Si df_clean n'est pas fourni, charge depuis output/dvf_clean.csv.
    """
    logger.info("=" * 60)
    logger.info("PHASE 3 — ENRICHISSEMENT")
    logger.info("=" * 60)

    if df_clean is None:
        clean_path = OUTPUT_DIR / "dvf_clean.csv"
        if not clean_path.exists():
            raise FileNotFoundError(f"Fichier introuvable : {clean_path}. Lancez d'abord la Phase 2.")
        df_clean = pd.read_csv(clean_path, dtype=str, low_memory=False)
        df_clean["valeur_fonciere"] = pd.to_numeric(df_clean["valeur_fonciere"], errors="coerce")
        df_clean["surface_reelle_bati"] = pd.to_numeric(df_clean["surface_reelle_bati"], errors="coerce")
        df_clean["date_mutation"] = pd.to_datetime(df_clean["date_mutation"], errors="coerce")
        df_clean["nombre_pieces_principales"] = pd.to_numeric(df_clean["nombre_pieces_principales"], errors="coerce")

    logger.info(f"Lignes en entrée : {len(df_clean):,}")

    df_ref_raw = load_communes_ref()
    df_ref = normalize_communes(df_ref_raw)

    df, df_rejets_geo = join_geographie(df_clean, df_ref)
    df = calcul_prix_m2(df)
    df = calcul_categorie_prix(df)
    df = decomposition_temporelle(df)
    df = calcul_grand_appart(df)

    enrich_path = OUTPUT_DIR / "dvf_enrich.csv"
    rejets_path = OUTPUT_DIR / "rejets_enrich.csv"

    df.to_csv(enrich_path, index=False, encoding="utf-8")
    logger.info(f"[output] dvf_enrich.csv → {len(df):,} lignes")

    if not df_rejets_geo.empty:
        df_rejets_geo.to_csv(rejets_path, index=False, encoding="utf-8")
        logger.info(f"[output] rejets_enrich.csv → {len(df_rejets_geo):,} lignes")
    else:
        pd.DataFrame(columns=["motif_rejet"]).to_csv(rejets_path, index=False)

    logger.info(f"PHASE 3 terminée — {len(df):,} mutations enrichies")
    return df


if __name__ == "__main__":
    run_enrich()