# Pipeline ETL — Demandes de Valeurs Foncières (DVF)

Pipeline ETL complet sur les données immobilières open data de data.gouv.fr.

## Prérequis

- Python 3.10+
- pip

## Installation

```bash
pip install -r requirements.txt
```

## Lancement rapide (pipeline complet)

```bash
# Paris (75), années 2022 et 2023, chargement complet
python 05_pipeline.py --mode full --year 2022 2023 --dept 75

# Lyon (69), chargement incrémental
python 05_pipeline.py --mode incremental --year 2023 --dept 69

# Plusieurs départements
python 05_pipeline.py --mode full --year 2022 2023 --dept 75 69 13
```

## Lancer les phases individuellement

```bash
python 01_extract.py --year 2022 2023 --dept 75
python 02_clean.py
python 03_enrich.py
python 04_load.py --mode full --db-path data/dw_dvf.db
```

## Structure du projet

```
projet_etl_dvf/
├── 01_extract.py        # Téléchargement + profilage
├── 02_clean.py          # Nettoyage + déduplication
├── 03_enrich.py         # Enrichissement géographique + indicateurs
├── 04_load.py           # Chargement SQLite (schéma en étoile)
├── 05_pipeline.py       # Orchestrateur + supervision + CLI
├── requirements.txt
├── README.md
├── data/
│   ├── raw/             # Fichiers CSV bruts (ne pas modifier)
│   ├── logs/            # pipeline.log
│   └── dw_dvf.db        # Entrepôt SQLite final
└── output/
    ├── dvf_clean.csv
    ├── dvf_enrich.csv
    ├── rejets_clean.csv
    ├── rejets_enrich.csv
    ├── profile_dvf.csv
    └── reports/         # Rapports HTML de supervision
```

## Requêtes SQL de démonstration

Ouvrir `data/dw_dvf.db` avec DB Browser for SQLite ou en ligne de commande :

```bash
sqlite3 data/dw_dvf.db
```

### Prix moyen par région et type de bien
```sql
SELECT c.nom_region, t.libelle_type,
       COUNT(*) AS nb_mutations,
       ROUND(AVG(f.valeur_fonciere)) AS prix_moyen,
       ROUND(AVG(f.prix_m2)) AS prix_m2_moyen
FROM fact_mutation f
JOIN dim_commune c ON f.sk_commune = c.sk_commune
JOIN dim_type_local t ON f.sk_type_local = t.sk_type_local
WHERE t.libelle_type IN ('Appartement', 'Maison')
GROUP BY c.nom_region, t.libelle_type
ORDER BY prix_moyen DESC;
```

### Évolution trimestrielle
```sql
SELECT d.annee, d.trimestre,
       COUNT(*) AS nb_transactions,
       ROUND(AVG(f.valeur_fonciere)) AS prix_moyen
FROM fact_mutation f
JOIN dim_date d ON f.sk_date = d.sk_date
GROUP BY d.annee, d.trimestre
ORDER BY d.annee, d.trimestre;
```

### Top 10 communes par volume
```sql
SELECT c.nom_commune, c.nom_departement,
       COUNT(*) AS nb_mutations,
       ROUND(AVG(f.prix_m2)) AS prix_m2_moyen,
       ROUND(SUM(f.valeur_fonciere)/1e6, 1) AS volume_millions_eur
FROM fact_mutation f
JOIN dim_commune c ON f.sk_commune = c.sk_commune
WHERE f.prix_m2 IS NOT NULL
GROUP BY c.sk_commune
ORDER BY nb_mutations DESC
LIMIT 10;
```

## Checklist de validation

- [ ] `python 05_pipeline.py --mode full --year 2022 --dept 75` s'exécute sans erreur
- [ ] Une 2e exécution en mode `incremental` ne double pas les données
- [ ] `fact_mutation` contient au moins 10 000 lignes
- [ ] Les 4 dimensions sont peuplées
- [ ] `etl_audit_log` trace toutes les phases
- [ ] `output/rejets_clean.csv` contient une colonne `motif_rejet`
