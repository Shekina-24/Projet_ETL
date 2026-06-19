"""
05_pipeline.py
Phase 5 — Automatisation et supervision
Orchestre les phases 1 à 4, avec retry (tenacity), journalisation dans
etl_audit_log, rapport de supervision et interface CLI.
"""

import importlib.util
import json
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "data" / "logs"
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

OUTPUT_DIR = BASE_DIR / "output"
REPORTS_DIR = OUTPUT_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Import dynamique des modules de phase (noms de fichiers commençant par un
# chiffre -> "import 01_extract" est invalide en Python, on passe par
# importlib).
# ---------------------------------------------------------------------------
def _import_module(filename: str, alias: str):
    spec = importlib.util.spec_from_file_location(alias, BASE_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


phase1 = _import_module("01_extract.py", "phase1_extract")
phase2 = _import_module("02_clean.py", "phase2_clean")
phase3 = _import_module("03_enrich.py", "phase3_enrich")
phase4 = _import_module("04_load.py", "phase4_load")


# ---------------------------------------------------------------------------
# Retry avec back-off exponentiel (1s -> 2s -> 4s, 3 tentatives max)
# Appliqué au téléchargement DVF (Phase 1) et au chargement SQLite (Phase 4),
# comme demandé par le sujet. On remplace la fonction dans le module importé :
# comme Python résout les noms globaux au moment de l'appel, le reste du
# code de 01_extract.py / 04_load.py (qui appelle la fonction par son nom nu)
# utilisera automatiquement la version "retry" sans qu'on ait à les modifier.
# ---------------------------------------------------------------------------
_retry_policy = dict(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)

phase1.download_dvf = retry(**_retry_policy, before_sleep=lambda s: logger.warning(
    f"[retry] download_dvf — tentative {s.attempt_number} échouée, nouvelle tentative…"
))(phase1.download_dvf)

phase4.run_load = retry(**_retry_policy, before_sleep=lambda s: logger.warning(
    f"[retry] run_load (SQLite) — tentative {s.attempt_number} échouée, nouvelle tentative…"
))(phase4.run_load)


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------
def log_audit(db_path: Path, phase: str, debut: datetime, fin: datetime,
              statut: str, nb_entree, nb_sortie, message: str | None):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """INSERT INTO etl_audit_log
               (phase, debut, fin, statut, nb_entree, nb_sortie, message)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (phase, debut.isoformat(), fin.isoformat(), statut, nb_entree, nb_sortie, message),
        )
        conn.commit()
    finally:
        conn.close()


def _run_step(db_path: Path, phase_name: str, nb_entree, func, *args, **kwargs):
    """
    Exécute une étape du pipeline, mesure sa durée, journalise le résultat
    dans etl_audit_log, et retourne (resultat, metrique_dict).
    Toute exception est journalisée puis ré-émise pour stopper le pipeline
    (les phases suivantes dépendent des sorties des phases précédentes).
    """
    debut = datetime.utcnow()
    logger.info(f"--- Démarrage phase « {phase_name} » ---")
    try:
        result = func(*args, **kwargs)
        fin = datetime.utcnow()
        nb_sortie = len(result) if hasattr(result, "__len__") else result
        log_audit(db_path, phase_name, debut, fin, "success", nb_entree, nb_sortie, None)
        metrique = {
            "phase": phase_name, "statut": "success",
            "debut": debut, "fin": fin, "duree_s": (fin - debut).total_seconds(),
            "nb_entree": nb_entree, "nb_sortie": nb_sortie, "message": None,
        }
        logger.info(
            f"--- Phase « {phase_name} » OK en {metrique['duree_s']:.1f}s "
            f"({nb_entree} -> {nb_sortie}) ---"
        )
        return result, metrique
    except (Exception, RetryError) as e:
        fin = datetime.utcnow()
        message = str(e)
        log_audit(db_path, phase_name, debut, fin, "error", nb_entree, None, message)
        metrique = {
            "phase": phase_name, "statut": "error",
            "debut": debut, "fin": fin, "duree_s": (fin - debut).total_seconds(),
            "nb_entree": nb_entree, "nb_sortie": None, "message": message,
        }
        logger.error(f"--- Phase « {phase_name} » ÉCHEC : {message} ---")
        return None, metrique


# ---------------------------------------------------------------------------
# Rapport de supervision
# ---------------------------------------------------------------------------
def _collect_anomalies() -> list[str]:
    """Relit les fichiers de sortie des phases 2/3 pour signaler des anomalies notables."""
    anomalies = []

    stats_path = OUTPUT_DIR / "stats_clean.json"
    if stats_path.exists():
        try:
            stats = json.loads(stats_path.read_text(encoding="utf-8"))
            nb_entree = stats.get("nb_lignes_brutes_entree", 0)
            nb_rejetees = stats.get("nb_lignes_rejetees", 0)
            if nb_entree:
                taux_rejet = 100 * nb_rejetees / nb_entree
                if taux_rejet > 5:
                    anomalies.append(
                        f"Taux de rejet en nettoyage élevé : {taux_rejet:.1f}% "
                        f"({nb_rejetees:,}/{nb_entree:,} lignes brutes)"
                    )
        except Exception:
            pass

    rejets_enrich_path = OUTPUT_DIR / "rejets_enrich.csv"
    enrich_path = OUTPUT_DIR / "dvf_enrich.csv"
    if rejets_enrich_path.exists() and enrich_path.exists():
        try:
            nb_rejets_geo = sum(1 for _ in open(rejets_enrich_path, encoding="utf-8")) - 1
            nb_enrich = sum(1 for _ in open(enrich_path, encoding="utf-8")) - 1
            total = nb_rejets_geo + nb_enrich
            if total > 0:
                taux_join = 100 * nb_enrich / total
                if taux_join < 95:
                    anomalies.append(
                        f"Taux de jointure géographique sous le seuil de 95% : {taux_join:.1f}%"
                    )
        except Exception:
            pass

    return anomalies


def generate_report(metriques: list[dict], mode: str, years, depts, db_path: Path) -> Path:
    """Génère un rapport texte récapitulatif dans output/reports/."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_DIR / f"rapport_pipeline_{timestamp}.txt"

    lignes = []
    lignes.append("=" * 70)
    lignes.append("RAPPORT DE SUPERVISION - PIPELINE ETL DVF")
    lignes.append("=" * 70)
    lignes.append(f"Date d'exécution : {datetime.now().isoformat()}")
    lignes.append(f"Mode             : {mode}")
    lignes.append(f"Années traitées  : {years}")
    lignes.append(f"Départements     : {depts}")
    lignes.append(f"Base SQLite      : {db_path}")
    lignes.append("")
    lignes.append("-" * 70)
    lignes.append("DÉTAIL PAR PHASE")
    lignes.append("-" * 70)

    duree_totale = 0.0
    statut_global = "success"
    for m in metriques:
        duree_totale += m["duree_s"]
        if m["statut"] == "error":
            statut_global = "error"
        lignes.append(f"\nPhase        : {m['phase']}")
        lignes.append(f"Statut       : {m['statut'].upper()}")
        lignes.append(f"Début        : {m['debut'].isoformat()}")
        lignes.append(f"Fin          : {m['fin'].isoformat()}")
        lignes.append(f"Durée        : {m['duree_s']:.2f}s")
        lignes.append(f"Lignes entrée: {m['nb_entree']}")
        lignes.append(f"Lignes sortie: {m['nb_sortie']}")
        if m["message"]:
            lignes.append(f"Erreur       : {m['message']}")

    lignes.append("")
    lignes.append("-" * 70)
    lignes.append(f"STATUT GLOBAL DU PIPELINE : {statut_global.upper()}")
    lignes.append(f"DURÉE TOTALE              : {duree_totale:.2f}s")
    lignes.append("-" * 70)

    anomalies = _collect_anomalies()
    lignes.append("")
    lignes.append("ANOMALIES DÉTECTÉES")
    lignes.append("-" * 70)
    if anomalies:
        for a in anomalies:
            lignes.append(f"  ⚠ {a}")
    else:
        lignes.append("  Aucune anomalie notable détectée.")

    report_path.write_text("\n".join(lignes), encoding="utf-8")
    logger.info(f"[rapport] Rapport de supervision généré : {report_path}")
    return report_path


# ---------------------------------------------------------------------------
# Orchestration principale
# ---------------------------------------------------------------------------
def run_pipeline(years: list[int], depts: list[str], mode: str, db_path: Path):
    logger.info("#" * 70)
    logger.info("PIPELINE ETL DVF — DÉMARRAGE")
    logger.info("#" * 70)

    # Initialisation de la base le plus tôt possible, pour que etl_audit_log
    # existe avant même la 1ère écriture (au cas où la Phase 1 échouerait).
    phase4.init_db(db_path)

    metriques = []

    # ---- Phase 1 : Extraction ----
    df_raw, m1 = _run_step(
        db_path, "extract", f"{len(years)} année(s) x {len(depts)} dépt(s)",
        phase1.run_extraction, years, depts,
    )
    metriques.append(m1)
    if df_raw is None:
        generate_report(metriques, mode, years, depts, db_path)
        logger.error("Pipeline arrêté : échec de la Phase 1 (extraction).")
        sys.exit(1)

    # ---- Phase 2 : Nettoyage ----
    df_clean, m2 = _run_step(
        db_path, "clean", len(df_raw),
        phase2.run_cleaning, df_raw,
    )
    metriques.append(m2)
    if df_clean is None:
        generate_report(metriques, mode, years, depts, db_path)
        logger.error("Pipeline arrêté : échec de la Phase 2 (nettoyage).")
        sys.exit(1)

    # ---- Phase 3 : Enrichissement ----
    df_enrich, m3 = _run_step(
        db_path, "enrich", len(df_clean),
        phase3.run_enrich, df_clean,
    )
    metriques.append(m3)
    if df_enrich is None:
        generate_report(metriques, mode, years, depts, db_path)
        logger.error("Pipeline arrêté : échec de la Phase 3 (enrichissement).")
        sys.exit(1)

    # ---- Phase 4 : Chargement ----
    nb_fact, m4 = _run_step(
        db_path, "load", len(df_enrich),
        phase4.run_load, df_enrich, db_path, mode,
    )
    metriques.append(m4)

    report_path = generate_report(metriques, mode, years, depts, db_path)

    statut_global = "success" if all(m["statut"] == "success" for m in metriques) else "error"
    logger.info("#" * 70)
    logger.info(f"PIPELINE ETL DVF — TERMINÉ ({statut_global.upper()})")
    logger.info(f"Rapport : {report_path}")
    logger.info("#" * 70)

    if statut_global == "error":
        sys.exit(1)

    return nb_fact


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline ETL DVF complet (Phases 1 à 4)")
    parser.add_argument("--year", type=int, nargs="+", default=[2022, 2023], help="Année(s) à traiter")
    parser.add_argument("--dept", type=str, nargs="+", default=["75"], help="Département(s) à traiter")
    parser.add_argument("--mode", choices=["full", "incremental"], default="full",
                         help="Rechargement complet ou incrémental")
    parser.add_argument("--db-path", default=str(phase4.DEFAULT_DB_PATH), help="Chemin vers la base SQLite")
    args = parser.parse_args()

    run_pipeline(args.year, args.dept, args.mode, Path(args.db_path))