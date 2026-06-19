"""
Lance ce script depuis le dossier du projet (où se trouve data/dw_dvf.db).
Il exécute les 3 requêtes de démonstration du sujet et affiche les résultats
au format Markdown, prêts à copier-coller.
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("data/dw_dvf.db")

QUERIES = {
    "Requête 1 — Prix moyen par région et type de bien": """
        SELECT
            c.nom_region,
            t.libelle_type,
            COUNT(*) AS nb_mutations,
            ROUND(AVG(f.valeur_fonciere)) AS prix_moyen,
            ROUND(AVG(f.prix_m2)) AS prix_m2_moyen
        FROM fact_mutation f
        JOIN dim_commune c ON f.sk_commune = c.sk_commune
        JOIN dim_type_local t ON f.sk_type_local = t.sk_type_local
        WHERE t.libelle_type IN ('Appartement', 'Maison')
          AND f.valeur_fonciere IS NOT NULL
        GROUP BY c.nom_region, t.libelle_type
        ORDER BY prix_moyen DESC;
    """,
    "Requête 2 — Évolution trimestrielle du marché": """
        SELECT
            d.annee,
            d.trimestre,
            COUNT(*) AS nb_transactions,
            ROUND(AVG(f.valeur_fonciere)) AS prix_moyen
        FROM fact_mutation f
        JOIN dim_date d ON f.sk_date = d.sk_date
        GROUP BY d.annee, d.trimestre
        ORDER BY d.annee, d.trimestre;
    """,
    "Requête 3 — Top 10 communes par volume de transactions": """
        SELECT
            c.nom_commune,
            c.nom_departement,
            COUNT(*) AS nb_mutations,
            ROUND(AVG(f.prix_m2)) AS prix_m2_moyen,
            ROUND(SUM(f.valeur_fonciere) / 1e6, 1) AS volume_millions_eur
        FROM fact_mutation f
        JOIN dim_commune c ON f.sk_commune = c.sk_commune
        WHERE f.prix_m2 IS NOT NULL
        GROUP BY c.sk_commune
        ORDER BY nb_mutations DESC
        LIMIT 10;
    """,
}


def main():
    if not DB_PATH.exists():
        print(f"ERREUR : {DB_PATH} introuvable. Lance ce script depuis le dossier du projet.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    for titre, sql in QUERIES.items():
        print("=" * 70)
        print(titre)
        print("=" * 70)
        rows = conn.execute(sql).fetchall()
        if not rows:
            print("(aucun résultat)")
            continue
        cols = rows[0].keys()
        print(" | ".join(cols))
        print("-" * 70)
        for r in rows:
            print(" | ".join(str(r[c]) for c in cols))
        print()

    conn.close()


if __name__ == "__main__":
    main()