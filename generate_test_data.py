"""
Génère un fichier DVF fictif mais réaliste pour tester le pipeline
sans connexion Internet vers data.gouv.fr.
Usage : python generate_test_data.py
"""
import random
import csv
from pathlib import Path
from datetime import date, timedelta

random.seed(42)

RAW_DIR = Path(__file__).parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

COMMUNES_75 = [
    ("75001", "75", "Paris 1er Arrondissement"),
    ("75002", "75", "Paris 2e Arrondissement"),
    ("75003", "75", "Paris 3e Arrondissement"),
    ("75004", "75", "Paris 4e Arrondissement"),
    ("75005", "75", "Paris 5e Arrondissement"),
    ("75006", "75", "Paris 6e Arrondissement"),
    ("75007", "75", "Paris 7e Arrondissement"),
    ("75008", "75", "Paris 8e Arrondissement"),
    ("75009", "75", "Paris 9e Arrondissement"),
    ("75010", "75", "Paris 10e Arrondissement"),
    ("75011", "75", "Paris 11e Arrondissement"),
    ("75012", "75", "Paris 12e Arrondissement"),
    ("75013", "75", "Paris 13e Arrondissement"),
    ("75014", "75", "Paris 14e Arrondissement"),
    ("75015", "75", "Paris 15e Arrondissement"),
    ("75016", "75", "Paris 16e Arrondissement"),
    ("75017", "75", "Paris 17e Arrondissement"),
    ("75018", "75", "Paris 18e Arrondissement"),
    ("75019", "75", "Paris 19e Arrondissement"),
    ("75020", "75", "Paris 20e Arrondissement"),
]

TYPES = [
    ("Appartement", 1, 2),
    ("Appartement", 2, 3),
    ("Appartement", 3, 4),
    ("Appartement", 4, 5),
    ("Maison", 3, 6),
    ("Dépendance", 0, 0),
]

VOIES = ["RUE DE LA PAIX", "AVENUE DES CHAMPS", "RUE SAINT-HONORÉ",
         "BOULEVARD HAUSSMANN", "RUE DE RIVOLI", "AVENUE MONTAIGNE",
         "RUE DU FAG", "PLACE DE LA BASTILLE", "RUE OBERKAMPF"]

COLUMNS = [
    "id_mutation", "date_mutation", "numero_disposition", "nature_mutation",
    "valeur_fonciere", "adresse_numero", "adresse_suffixe", "adresse_nom_voie",
    "code_postal", "code_commune", "nom_commune", "code_departement",
    "id_parcelle", "nombre_lots", "code_type_local", "type_local",
    "surface_reelle_bati", "nombre_pieces_principales", "surface_terrain",
    "longitude", "latitude",
]

def random_date(year):
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_mutations(year, n=6000):
    rows = []
    mut_id = 1

    for _ in range(n):
        commune = random.choice(COMMUNES_75)
        type_info = random.choice(TYPES)
        type_local, nb_pieces_min, nb_pieces_max = type_info

        nb_pieces = random.randint(nb_pieces_min, max(nb_pieces_min, nb_pieces_max))

        # Prix selon type et arrondissement
        code_commune = commune[0]
        arr = int(code_commune[-2:])
        base_prix_m2 = {
            1: 14000, 2: 13000, 3: 13500, 4: 14000, 5: 13000,
            6: 15000, 7: 15500, 8: 14500, 9: 12000, 10: 11000,
            11: 11000, 12: 10500, 13: 10000, 14: 10500, 15: 11000,
            16: 13000, 17: 12000, 18: 10000, 19: 9500, 20: 9500,
        }.get(arr, 11000)

        if type_local == "Appartement":
            surface = random.uniform(20, 150)
        elif type_local == "Maison":
            surface = random.uniform(60, 250)
        else:
            surface = None

        if surface:
            prix = round(surface * base_prix_m2 * random.uniform(0.8, 1.2))
        else:
            prix = random.randint(5000, 50000)

        # Quelques anomalies intentionnelles pour tester le pipeline
        nature = "Vente"
        if random.random() < 0.03:
            nature = "Vente en l'état futur d'achèvement"  # sera rejeté
        if random.random() < 0.01:
            prix = 100  # sous le seuil min → rejeté

        row = {
            "id_mutation": f"{year}-{mut_id}",
            "date_mutation": random_date(year).isoformat(),
            "numero_disposition": "1",
            "nature_mutation": nature,
            "valeur_fonciere": prix,
            "adresse_numero": random.randint(1, 100),
            "adresse_suffixe": random.choice(["", "BIS", "TER", ""]),
            "adresse_nom_voie": random.choice(VOIES),
            "code_postal": code_commune.replace("75", "750"),
            "code_commune": code_commune,
            "nom_commune": commune[2],
            "code_departement": commune[1],
            "id_parcelle": f"{code_commune}0A{random.randint(100, 999):04d}",
            "nombre_lots": random.choice([0, 1, 2]),
            "code_type_local": {"Appartement": 2, "Maison": 1, "Dépendance": 3}.get(type_local, 2),
            "type_local": type_local,
            "surface_reelle_bati": round(surface, 2) if surface else "",
            "nombre_pieces_principales": nb_pieces,
            "surface_terrain": round(random.uniform(0, 50), 1) if random.random() < 0.3 else "",
            "longitude": round(2.3 + random.uniform(-0.05, 0.05), 6),
            "latitude": round(48.87 + random.uniform(-0.05, 0.05), 6),
        }

        # Simuler multi-lignes pour ~5% des mutations (dépendance attachée)
        rows.append(row)
        if random.random() < 0.05:
            dep_row = row.copy()
            dep_row["type_local"] = "Dépendance"
            dep_row["surface_reelle_bati"] = ""
            dep_row["nombre_pieces_principales"] = 0
            dep_row["code_type_local"] = 3
            rows.append(dep_row)

        mut_id += 1

    return rows

for year in [2022, 2023]:
    rows = generate_mutations(year, n=6000)
    out = RAW_DIR / f"dvf_{year}_75.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"✅ Généré : {out} ({len(rows)} lignes)")

print("\nDonnées de test prêtes — tu peux maintenant lancer le pipeline !")
