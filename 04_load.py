"""
Phase 4 — Chargement dans l'entrepôt de données SQLite (dw_dvf.db)
Implémente le schéma en étoile, le staging, le chargement incrémental et la validation.
"""

import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

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

DATA_DIR = Path(__file__).parent / "data"
OUTPUT_DIR = Path(__file__).parent / "output"
DEFAULT_DB_PATH = DATA_DIR / "dw_dvf.db"

DDL = """
CREATE TABLE IF NOT EXISTS dim_commune (
    sk_commune        INTEGER PRIMARY KEY AUTOINCREMENT,
    code_commune      TEXT NOT NULL UNIQUE,
    nom_commune       TEXT,
    code_postal       TEXT,
    code_departement  TEXT,
    nom_departement   TEXT,
    code_region       TEXT,
    nom_region        TEXT,
    latitude          REAL,
    longitude         REAL
);

CREATE TABLE IF NOT EXISTS dim_type_local (
    sk_type_local  INTEGER PRIMARY KEY AUTOINCREMENT,
    code_type      TEXT NOT NULL UNIQUE,
    libelle_type   TEXT
);

CREATE TABLE IF NOT EXISTS dim_nature_mutation (
    sk_nature  INTEGER PRIMARY KEY AUTOINCREMENT,
    nature     TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_date (
    sk_date       INTEGER PRIMARY KEY,
    date_iso      TEXT NOT NULL,
    annee         INTEGER,
    semestre      INTEGER,
    trimestre     INTEGER,
    mois          INTEGER,
    libelle_mois  TEXT
);

CREATE TABLE IF NOT EXISTS fact_mutation (
    sk_mutation    INTEGER PRIMARY KEY AUTOINCREMENT,
    sk_commune     INTEGER REFERENCES dim_commune(sk_commune),
    sk_type_local  INTEGER REFERENCES dim_type_local(sk_type_local),
    sk_nature      INTEGER REFERENCES dim_nature_mutation(sk_nature),
    sk_date        INTEGER REFERENCES dim_date(sk_date),
    id_mutation    TEXT NOT NULL UNIQUE,
    valeur_fonciere REAL,
    surface_bati    REAL,
    surface_terrain REAL,
    nb_pieces       INTEGER,
    nb_lots         INTEGER,
    prix_m2         REAL,
    categorie_prix  TEXT
);

CREATE TABLE IF NOT EXISTS fact_mutation_staging (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    loaded_at      TEXT,
    id_mutation    TEXT,
    date_mutation  TEXT,
    nature_mutation TEXT,
    valeur_fonciere REAL,
    code_commune    TEXT,
    nom_commune     TEXT,
    code_departement TEXT,
    nom_departement TEXT,
    code_region     TEXT,
    nom_region      TEXT,
    type_local      TEXT,
    surface_reelle_bati REAL,
    surface_terrain REAL,
    nombre_pieces_principales INTEGER,
    nombre_lots     INTEGER,
    prix_m2         REAL,
    categorie_prix  TEXT,
    annee           INTEGER,
    mois            INTEGER,
    trimestre       INTEGER,
    semestre        INTEGER
);

CREATE TABLE IF NOT EXISTS etl_audit_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    phase        TEXT,
    debut        TEXT,
    fin          TEXT,
    statut       TEXT,
    nb_entree    INTEGER,
    nb_sortie    INTEGER,
    message      TEXT
);

CREATE TABLE IF NOT EXISTS etl_rejets (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    phase        TEXT,
    id_mutation  TEXT,
    motif        TEXT,
    inserted_at  TEXT
);

CREATE TABLE IF NOT EXISTS etl_watermark (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    derniere_date   TEXT,
    updated_at      TEXT
);
"""

MOIS_FR = {
    1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril",
    5: "Mai", 6: "Juin", 7: "Juillet", 8: "Août",
    9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre",
}


def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db(db_path: Path = DEFAULT_DB_PATH):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with get_connection(db_path) as conn:
        conn.executescript(DDL)
    logger.info(f"[init_db] Base initialisée : {db_path}")


def _normalize_code_commune(df: pd.DataFrame) -> pd.DataFrame:
    """
    Force code_commune en texte sur 5 caracteres. Indispensable : sans ca,
    un CSV relu sans dtype explicite fait inferer un int64 a pandas (ex.
    69123), alors que la colonne stockee en SQLite (affinite TEXT) revient
    en object/str -> echec du merge ("merge on int64 and object columns").
    """
    df = df.copy()
    df["code_commune"] = df["code_commune"].astype(str).str.strip().str.zfill(5)
    return df


def load_staging(df: pd.DataFrame, db_path: Path):
    logger.info("[staging] Chargement en staging…")
    loaded_at = datetime.utcnow().isoformat()

    staging_cols = [
        "id_mutation", "date_mutation", "nature_mutation", "valeur_fonciere",
        "code_commune", "nom_commune", "code_departement", "nom_departement",
        "code_region", "nom_region", "type_local", "surface_reelle_bati",
        "surface_terrain", "nombre_pieces_principales", "nombre_lots",
        "prix_m2", "categorie_prix", "annee", "mois", "trimestre", "semestre",
    ]
    available = [c for c in staging_cols if c in df.columns]
    df_stage = df[available].copy()
    df_stage["loaded_at"] = loaded_at

    with get_connection(db_path) as conn:
        df_stage.to_sql("fact_mutation_staging", conn, if_exists="append", index=False)

    logger.info(f"[staging] {len(df_stage):,} lignes insérées en staging")


def load_dim_commune(df: pd.DataFrame, db_path: Path):
    cols_needed = ["code_commune", "nom_commune", "code_departement",
                   "nom_departement", "code_region", "nom_region"]
    available = [c for c in cols_needed if c in df.columns]
    communes = df[available].drop_duplicates(subset=["code_commune"]).copy()
    for c in cols_needed:
        if c not in communes.columns:
            communes[c] = None

    with get_connection(db_path) as conn:
        for _, row in communes.iterrows():
            conn.execute("""
                INSERT INTO dim_commune (code_commune, nom_commune, code_departement,
                    nom_departement, code_region, nom_region)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(code_commune) DO UPDATE SET
                    nom_commune = excluded.nom_commune,
                    nom_departement = excluded.nom_departement,
                    nom_region = excluded.nom_region
            """, (
                row.get("code_commune"), row.get("nom_commune"),
                row.get("code_departement"), row.get("nom_departement"),
                row.get("code_region"), row.get("nom_region"),
            ))
        nb = conn.execute("SELECT COUNT(*) FROM dim_commune").fetchone()[0]
    logger.info(f"[dim_commune] {nb:,} communes en base")


def load_dim_type_local(df: pd.DataFrame, db_path: Path):
    types = df["type_local"].dropna().unique()
    with get_connection(db_path) as conn:
        for t in types:
            conn.execute(
                "INSERT OR IGNORE INTO dim_type_local (code_type, libelle_type) VALUES (?, ?)",
                (t, t)
            )
    logger.info(f"[dim_type_local] {len(types)} types chargés")


def load_dim_nature_mutation(df: pd.DataFrame, db_path: Path):
    natures = df["nature_mutation"].dropna().unique()
    with get_connection(db_path) as conn:
        for n in natures:
            conn.execute(
                "INSERT OR IGNORE INTO dim_nature_mutation (nature) VALUES (?)",
                (n,)
            )
    logger.info(f"[dim_nature_mutation] {len(natures)} natures chargées")


def load_dim_date(df: pd.DataFrame, db_path: Path):
    dates = pd.to_datetime(df["date_mutation"], errors="coerce").dropna().dt.date.unique()
    with get_connection(db_path) as conn:
        for d in dates:
            dt = pd.Timestamp(d)
            sk = int(dt.strftime("%Y%m%d"))
            mois = dt.month
            conn.execute("""
                INSERT OR IGNORE INTO dim_date
                    (sk_date, date_iso, annee, semestre, trimestre, mois, libelle_mois)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                sk, d.isoformat(), dt.year,
                1 if mois <= 6 else 2,
                dt.quarter, mois,
                MOIS_FR.get(mois, str(mois)),
            ))
    logger.info(f"[dim_date] {len(dates)} dates chargées")


def load_fact_mutation(df: pd.DataFrame, db_path: Path, mode: str = "full") -> int:
    df = _normalize_code_commune(df)

    with get_connection(db_path) as conn:
        if mode == "incremental":
            row = conn.execute(
                "SELECT derniere_date FROM etl_watermark ORDER BY id DESC LIMIT 1"
            ).fetchone()
            watermark = row[0] if row else None
            if watermark:
                logger.info(f"[fact] Mode incrémental — watermark : {watermark}")
                df = df[df["date_mutation"].astype(str) > watermark].copy()
                if df.empty:
                    logger.info("[fact] Aucune nouvelle donnée depuis le dernier watermark.")
                    return 0

        communes = pd.read_sql("SELECT sk_commune, code_commune FROM dim_commune", conn)
        types = pd.read_sql("SELECT sk_type_local, code_type FROM dim_type_local", conn)
        natures = pd.read_sql("SELECT sk_nature, nature FROM dim_nature_mutation", conn)
        communes["code_commune"] = communes["code_commune"].astype(str).str.zfill(5)

        df2 = df.copy()
        df2["date_mutation"] = pd.to_datetime(df2["date_mutation"], errors="coerce")

        df2 = df2.merge(communes, on="code_commune", how="left")
        df2 = df2.merge(types, left_on="type_local", right_on="code_type", how="left")
        df2 = df2.merge(natures, left_on="nature_mutation", right_on="nature", how="left")
        df2["sk_date"] = df2["date_mutation"].dt.strftime("%Y%m%d").astype("Int64")

        nb_inserted = 0
        nb_ignored = 0
        rejets = []

        for _, row in df2.iterrows():
            if pd.isna(row.get("sk_commune")):
                rejets.append((row.get("id_mutation"), "sk_commune non résolu"))
                continue
            try:
                cur = conn.execute("""
                    INSERT OR IGNORE INTO fact_mutation
                        (sk_commune, sk_type_local, sk_nature, sk_date,
                         id_mutation, valeur_fonciere, surface_bati, surface_terrain,
                         nb_pieces, nb_lots, prix_m2, categorie_prix)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    _safe_int(row.get("sk_commune")),
                    _safe_int(row.get("sk_type_local")),
                    _safe_int(row.get("sk_nature")),
                    _safe_int(row.get("sk_date")),
                    row.get("id_mutation"),
                    _safe_float(row.get("valeur_fonciere")),
                    _safe_float(row.get("surface_reelle_bati")),
                    _safe_float(row.get("surface_terrain")),
                    _safe_int(row.get("nombre_pieces_principales")),
                    _safe_int(row.get("nombre_lots")),
                    _safe_float(row.get("prix_m2")),
                    row.get("categorie_prix"),
                ))
                if cur.rowcount:
                    nb_inserted += 1
                else:
                    nb_ignored += 1
            except Exception as e:
                rejets.append((row.get("id_mutation"), str(e)))

        now = datetime.utcnow().isoformat()
        for id_mut, motif in rejets:
            conn.execute(
                "INSERT INTO etl_rejets (phase, id_mutation, motif, inserted_at) VALUES (?, ?, ?, ?)",
                ("phase4", id_mut, motif, now)
            )

        max_date = df["date_mutation"].astype(str).max()
        conn.execute(
            "INSERT INTO etl_watermark (derniere_date, updated_at) VALUES (?, ?)",
            (max_date, now)
        )

        logger.info(
            f"[fact] {nb_inserted:,} nouvelles mutations insérées / "
            f"{nb_ignored:,} déjà présentes (idempotence) / {len(rejets):,} rejetées"
        )
        return nb_inserted


def _safe_int(v):
    try:
        if pd.isna(v):
            return None
        return int(v)
    except Exception:
        return None


def _safe_float(v):
    try:
        if pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None


def validate(db_path: Path, nb_staging: int):
    with get_connection(db_path) as conn:
        nb_fact = conn.execute("SELECT COUNT(*) FROM fact_mutation").fetchone()[0]
        nb_fk_null = conn.execute(
            "SELECT COUNT(*) FROM fact_mutation WHERE sk_commune IS NULL"
        ).fetchone()[0]

    logger.info(f"[validation] fact_mutation : {nb_fact:,} lignes")
    logger.info(f"[validation] FK null (sk_commune) : {nb_fk_null}")
    if nb_fk_null > 0:
        logger.warning(f"[validation] {nb_fk_null} lignes avec sk_commune NULL dans fact_mutation")
    return nb_fact


def run_load(df_enrich: pd.DataFrame = None, db_path: Path = DEFAULT_DB_PATH, mode: str = "full"):
    logger.info("=" * 60)
    logger.info("PHASE 4 — CHARGEMENT DW")
    logger.info("=" * 60)

    if df_enrich is None:
        enrich_path = OUTPUT_DIR / "dvf_enrich.csv"
        if not enrich_path.exists():
            raise FileNotFoundError(f"Fichier introuvable : {enrich_path}. Lancez d'abord la Phase 3.")
        df_enrich = pd.read_csv(
            enrich_path,
            low_memory=False,
            dtype={
                "id_mutation": str,
                "code_commune": str,
                "code_departement": str,
                "code_region": str,
                "type_local": str,
                "nature_mutation": str,
            },
        )
        df_enrich["valeur_fonciere"] = pd.to_numeric(df_enrich["valeur_fonciere"], errors="coerce")
        df_enrich["surface_reelle_bati"] = pd.to_numeric(df_enrich["surface_reelle_bati"], errors="coerce")
        df_enrich["date_mutation"] = pd.to_datetime(df_enrich["date_mutation"], errors="coerce")

    df_enrich = _normalize_code_commune(df_enrich)

    logger.info(f"Lignes en entrée : {len(df_enrich):,} | Mode : {mode}")

    init_db(db_path)
    load_staging(df_enrich, db_path)
    nb_staging = len(df_enrich)

    load_dim_commune(df_enrich, db_path)
    load_dim_type_local(df_enrich, db_path)
    load_dim_nature_mutation(df_enrich, db_path)
    load_dim_date(df_enrich, db_path)

    nb_insere = load_fact_mutation(df_enrich, db_path, mode=mode)
    nb_fact = validate(db_path, nb_staging)

    logger.info(f"PHASE 4 terminée — {nb_insere:,} nouvelles mutations dans fact_mutation (total: {nb_fact:,})")
    return nb_fact


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Phase 4 — Chargement DW")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Chemin vers la base SQLite")
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    args = parser.parse_args()
    run_load(db_path=Path(args.db_path), mode=args.mode)