"""
===============================================================
  ETL TRANSFORM — Datamart Sun Séjour
  Version  : 2.0 — Production
  Phase    : 1 (Extraction) + 2 (Transformation & Modélisation)
---------------------------------------------------------------
  Pipeline :
    1. Lecture des fichiers source (.xlsx transfert / .xls excursion)
    2. Extraction des dimensions et tables de faits
    3. Fusion des dimensions communes (client, hotel, currency)
    4. Mapping des IDs dans les tables de faits
    5. Contrôle qualité (doublons, FK, nulls, montants)
    6. Export vers etl_ready/dim_final/ et etl_ready/fact_final/
    7. Logs horodatés fichier + console
===============================================================
"""

import os
import logging
import traceback
import pandas as pd
from datetime import datetime
from config import DATA_DIR
# ================================================================
#  CONFIGURATION
# ================================================================
BASE_DIR            = os.path.dirname(os.path.abspath(__file__))
TRANSFERT_INPUT_DIR = os.path.join(BASE_DIR, DATA_DIR["transfert_data"])
EXCURSION_INPUT_DIR = os.path.join(BASE_DIR, DATA_DIR["excursion_data"])

FACT_FINAL_DIR = os.path.join(BASE_DIR, "etl_ready", "fact_final")
DIM_FINAL_DIR  = os.path.join(BASE_DIR, "etl_ready", "dim_final")
LOG_DIR        = os.path.join(BASE_DIR, "logs")

# Dossiers intermédiaires
TRANSFERT_FACT_DIR = os.path.join(BASE_DIR, "etl_ready", "fact_transfert")
TRANSFERT_DIM_DIR  = os.path.join(BASE_DIR, "etl_ready", "dim_transfert")
EXCURSION_FACT_DIR = os.path.join(BASE_DIR, "etl_ready", "fact_excursion")
EXCURSION_DIM_DIR  = os.path.join(BASE_DIR, "etl_ready", "dim_excursion")

for d in [FACT_FINAL_DIR, DIM_FINAL_DIR, LOG_DIR,
          TRANSFERT_FACT_DIR, TRANSFERT_DIM_DIR,
          EXCURSION_FACT_DIR, EXCURSION_DIM_DIR]:
    os.makedirs(d, exist_ok=True)

# ── Colonnes source TRANSFERT ────────────────────────────────────
TRANSFERT_DIM_COLUMNS = {
    "dim_date":     ["Date", "Heure"],
    "dim_client":   ["Client"],
    "dim_hotel":    ["Hotel"],
    "dim_airport":  ["Airport", "Centre"],
    "dim_flight":   ["N° Vol"],
    "dim_currency": ["Monnaie", "Taux"],
    "dim_status":   ["Position", "Evénement"],
}

TRANSFERT_FACT_COLUMNS = [
    "N° Reservation", "N° Dossier", "Mission",
    "Pax", "A", "E", "B", "Montant", "Montant Local",
]

# ── Colonnes source EXCURSION ────────────────────────────────────
EXCURSION_DIM_COLUMNS = {
    "dim_date_excursion":  ["Date Réservation", "Date d'arrivée", "Date Excursion"],
    "dim_client":          ["Client"],
    "dim_hotel":           ["Hotel"],
    "dim_currency":        ["Monnaie"],
    "dim_excursion":       ["Excursion"],
    "dim_vendeur":         ["Vendeur"],
    "dim_status_excursion":["Situation", "Etat"],
}

EXCURSION_FACT_COLUMNS = [
    "N° Ticket", "Réf Réservation", "Code Facture", "Pos",
    "Nbr Adt", "Nbr Enf", "Nbr bb",
    "Nbr Adt Grt", "Nbr Enf Grt", "Nbr bb Grt",
    "Montant",
]

# ================================================================
#  LOGGING — fichier horodaté + console
# ================================================================
_log_file = os.path.join(LOG_DIR, f"etl_transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
_logger = logging.getLogger("ETL_TRANSFORM")

def log(msg, level="info"):
    getattr(_logger, level)(msg)

def section(title):
    log("=" * 60)
    log(f"  {title}")
    log("=" * 60)

# ================================================================
#  PHASE 1 — EXTRACTION TRANSFERT
# ================================================================
def extract_transfert():
    section("PHASE 1 — EXTRACTION TRANSFERT")

    files = sorted([f for f in os.listdir(TRANSFERT_INPUT_DIR) if f.lower().endswith(".xlsx")])
    if not files:
        log(f"  ⚠️  Aucun fichier .xlsx dans {TRANSFERT_INPUT_DIR}", "warning")
        return {}

    log(f"  {len(files)} fichier(s) trouvé(s) : {', '.join(files)}")

    dim_buffers = {dim: [] for dim in TRANSFERT_DIM_COLUMNS}
    nb_total    = 0

    for file in files:
        path = os.path.join(TRANSFERT_INPUT_DIR, file)
        try:
            df     = pd.read_excel(path)
            period = os.path.splitext(file)[0]
            log(f"  [{file}] {len(df):,} lignes lues.")

            # Vérification colonnes manquantes
            all_cols    = TRANSFERT_FACT_COLUMNS + sum(TRANSFERT_DIM_COLUMNS.values(), [])
            missing     = [c for c in all_cols if c not in df.columns]
            if missing:
                log(f"  [{file}] ⚠️  Colonnes manquantes : {missing}", "warning")

            # Fact intermédiaire
            available   = [c for c in all_cols if c in df.columns]
            fact_df     = df[available]
            fact_path   = os.path.join(TRANSFERT_FACT_DIR, f"fact_transfers_{period}.csv")
            fact_df.to_csv(fact_path, index=False)
            nb_total   += len(fact_df)

            # Dimensions
            for dim, cols in TRANSFERT_DIM_COLUMNS.items():
                present = [c for c in cols if c in df.columns]
                if present:
                    dim_buffers[dim].append(df[present])

        except Exception as e:
            log(f"  [{file}] ❌ Erreur lecture : {e}", "error")
            traceback.print_exc()

    # Export dimensions brutes
    for dim, dfs in dim_buffers.items():
        if dfs:
            dim_df = pd.concat(dfs).drop_duplicates().reset_index(drop=True)
            dim_df.to_csv(os.path.join(TRANSFERT_DIM_DIR, f"{dim}.csv"), index=False)

    log(f"  ✅ Extraction TRANSFERT terminée — {nb_total:,} lignes fact totales.")
    return dim_buffers

# ================================================================
#  PHASE 1 — EXTRACTION EXCURSION
# ================================================================
def extract_excursion():
    section("PHASE 1 — EXTRACTION EXCURSION")

    files = sorted([f for f in os.listdir(EXCURSION_INPUT_DIR) if f.lower().endswith(".xls")])
    if not files:
        log(f"  ⚠️  Aucun fichier .xls dans {EXCURSION_INPUT_DIR}", "warning")
        return {}

    log(f"  {len(files)} fichier(s) trouvé(s) : {', '.join(files)}")

    dim_buffers = {dim: [] for dim in EXCURSION_DIM_COLUMNS}
    nb_total    = 0

    for file in files:
        path = os.path.join(EXCURSION_INPUT_DIR, file)
        try:
            df     = pd.read_html(path)[0]
            period = os.path.splitext(file)[0]
            log(f"  [{file}] {len(df):,} lignes lues.")

            all_cols = EXCURSION_FACT_COLUMNS + sum(EXCURSION_DIM_COLUMNS.values(), [])
            missing  = [c for c in all_cols if c not in df.columns]
            if missing:
                log(f"  [{file}] ⚠️  Colonnes manquantes : {missing}", "warning")

            available = [c for c in all_cols if c in df.columns]
            fact_df   = df[available]
            fact_path = os.path.join(EXCURSION_FACT_DIR, f"fact_excursion_{period}.csv")
            fact_df.to_csv(fact_path, index=False)
            nb_total += len(fact_df)

            for dim, cols in EXCURSION_DIM_COLUMNS.items():
                present = [c for c in cols if c in df.columns]
                if present:
                    dim_buffers[dim].append(df[present])

        except Exception as e:
            log(f"  [{file}] ❌ Erreur lecture : {e}", "error")
            traceback.print_exc()

    for dim, dfs in dim_buffers.items():
        if dfs:
            dim_df = pd.concat(dfs).drop_duplicates().reset_index(drop=True)
            dim_df.to_csv(os.path.join(EXCURSION_DIM_DIR, f"{dim}.csv"), index=False)

    log(f"  ✅ Extraction EXCURSION terminée — {nb_total:,} lignes fact totales.")
    return dim_buffers

# ================================================================
#  PHASE 2 — TRANSFORMATION & MODÉLISATION
# ================================================================
def transform():
    section("PHASE 2 — TRANSFORMATION & MODÉLISATION")

    # ── Concaténation facts TRANSFERT ───────────────────────────
    log("  Chargement facts TRANSFERT...")
    dfs_t = [pd.read_csv(os.path.join(TRANSFERT_FACT_DIR, f))
             for f in os.listdir(TRANSFERT_FACT_DIR) if f.endswith(".csv")]
    if not dfs_t:
        log("  ❌ Aucun fichier fact_transfert intermédiaire trouvé.", "error")
        return None, None, None

    fact_transfert = pd.concat(dfs_t, ignore_index=True)
    fact_transfert = fact_transfert.rename(columns={
        "N° Reservation": "Num_Reservation",
        "N° Vol":         "Num_Vol",
        "N° Dossier":     "Num_Dossier",
    })
    fact_transfert.fillna({
        "Heure":           "00:00:00",
        "Num_Vol":         "UNKNOWN",
        "Num_Reservation": "NO_RESERVATION",
        "Num_Dossier":     "NO_DOSSIER",
        "Mission":         -1,
        "Monnaie":         "UNKNOWN",
        "Montant":         0,
        "Montant Local":   0,
        "Taux":            1.0,
    }, inplace=True)
    log(f"  fact_transfert : {len(fact_transfert):,} lignes brutes.")

    # ── Concaténation facts EXCURSION ────────────────────────────
    log("  Chargement facts EXCURSION...")
    dfs_e = [pd.read_csv(os.path.join(EXCURSION_FACT_DIR, f))
             for f in os.listdir(EXCURSION_FACT_DIR) if f.endswith(".csv")]
    if not dfs_e:
        log("  ❌ Aucun fichier fact_excursion intermédiaire trouvé.", "error")
        return None, None, None

    fact_excursion = pd.concat(dfs_e, ignore_index=True)
    fact_excursion = fact_excursion.rename(columns={
        "N° Ticket":       "Num_Ticket",
        "Réf Réservation": "Num_Reservation",
        "N° Chambre":      "Num_Chambre",
        "Date Réservation":"Date_Reservation",
        "Date d'arrivée":  "Date_Arrivee",
        "Date Excursion":  "Date_Excursion",
        "Situation":       "Status",
        "Etat":            "Etat_Excursion",
        "Code Facture":    "Code_Facture",
        "Pos":             "Position",
        "Monnaie":         "Currency",
    })
    fact_excursion.fillna({
        "Date_Reservation": "2000-01-01",
        "Date_Arrivee":     "2000-01-01",
        "Date_Excursion":   "2000-01-01",
        "Num_Reservation":  "NO_RESERVATION",
        "Num_Ticket":       "NO_TICKET",
        "Code_Facture":     "UNKNOWN",
        "Position":         "UNKNOWN",
        "Montant":          0,
        "Currency":         "UNKNOWN",
        "Num_Chambre":      "UNKNOWN",
    }, inplace=True)
    log(f"  fact_excursion : {len(fact_excursion):,} lignes brutes.")

    # ── Dimensions communes ──────────────────────────────────────
    log("  Construction dimensions communes...")

    dim_client = pd.concat([
        fact_transfert[["Client"]], fact_excursion[["Client"]]
    ]).drop_duplicates().reset_index(drop=True)
    dim_client["client_id"] = dim_client.index + 1

    dim_hotel = pd.concat([
        fact_transfert[["Hotel"]], fact_excursion[["Hotel"]]
    ]).drop_duplicates().reset_index(drop=True)
    dim_hotel["hotel_id"] = dim_hotel.index + 1

    dim_currency = pd.concat([
        fact_transfert[["Monnaie", "Taux"]],
        fact_excursion[["Currency"]].rename(columns={"Currency": "Monnaie"}).assign(Taux=1.0)
    ]).drop_duplicates().reset_index(drop=True)
    dim_currency["currency_id"] = dim_currency.index + 1

    # ── Dimensions spécifiques TRANSFERT ─────────────────────────
    log("  Construction dimensions TRANSFERT...")

    dim_date = fact_transfert[["Date", "Heure"]].drop_duplicates().reset_index(drop=True)
    dim_date["date_id"] = dim_date.index + 1

    dim_airport = fact_transfert[["Airport", "Centre"]].drop_duplicates().reset_index(drop=True)
    dim_airport["airport_id"] = dim_airport.index + 1

    dim_flight = fact_transfert[["Num_Vol"]].drop_duplicates().reset_index(drop=True)
    dim_flight["flight_id"] = dim_flight.index + 1

    dim_status = fact_transfert[["Position", "Evénement"]].drop_duplicates().reset_index(drop=True)
    dim_status["status_id"] = dim_status.index + 1

    # ── Dimensions spécifiques EXCURSION ─────────────────────────
    log("  Construction dimensions EXCURSION...")

    dim_date_exc = fact_excursion[["Date_Reservation", "Date_Arrivee", "Date_Excursion"]].drop_duplicates().reset_index(drop=True)
    dim_date_exc["date_id"] = dim_date_exc.index + 1

    dim_excursion = fact_excursion[["Excursion"]].drop_duplicates().reset_index(drop=True)
    dim_excursion["excursion_id"] = dim_excursion.index + 1

    dim_vendeur = fact_excursion[["Vendeur"]].drop_duplicates().reset_index(drop=True)
    dim_vendeur["vendeur_id"] = dim_vendeur.index + 1

    dim_status_exc = fact_excursion[["Status", "Etat_Excursion"]].drop_duplicates().reset_index(drop=True)
    dim_status_exc["status_id"] = dim_status_exc.index + 1

    # ── Mapping IDs → fact_transfert ─────────────────────────────
    log("  Mapping IDs → fact_transfert...")
    fact_transfert_final = (
        fact_transfert
        .merge(dim_client,   on="Client",            how="left")
        .merge(dim_hotel,    on="Hotel",             how="left")
        .merge(dim_currency, on=["Monnaie", "Taux"], how="left")
        .merge(dim_date,     on=["Date", "Heure"],   how="left")
        .merge(dim_airport,  on=["Airport", "Centre"],how="left")
        .merge(dim_flight,   on="Num_Vol",           how="left")
        .merge(dim_status,   on=["Position", "Evénement"], how="left")
    )
    fact_transfert_final.drop(
        columns=["Client", "Hotel", "Monnaie", "Taux", "Date", "Heure",
                 "Airport", "Centre", "Num_Vol", "Position", "Evénement"],
        inplace=True, errors="ignore"
    )
    before = len(fact_transfert_final)
    fact_transfert_final.drop_duplicates(subset=["Num_Reservation"], keep="first", inplace=True)
    fact_transfert_final.reset_index(drop=True, inplace=True)
    if before > len(fact_transfert_final):
        log(f"  ⚠️  fact_transfert : {before - len(fact_transfert_final):,} doublons Num_Reservation supprimés.", "warning")
    log(f"  fact_transfert_final : {len(fact_transfert_final):,} lignes.")

    # ── Mapping IDs → fact_excursion ─────────────────────────────
    log("  Mapping IDs → fact_excursion...")
    fact_excursion_final = (
        fact_excursion
        .merge(dim_client,   on="Client",            how="left")
        .merge(dim_hotel,    on="Hotel",             how="left")
        .merge(dim_currency, left_on="Currency", right_on="Monnaie", how="left")
        .merge(dim_date_exc, on=["Date_Reservation", "Date_Arrivee", "Date_Excursion"], how="left")
        .merge(dim_excursion,on="Excursion",          how="left")
        .merge(dim_vendeur,  on="Vendeur",            how="left")
        .merge(dim_status_exc, on=["Status", "Etat_Excursion"], how="left")
    )
    fact_excursion_final.drop(
        columns=["Client", "Hotel", "Currency", "Monnaie",
                 "Date_Reservation", "Date_Arrivee", "Date_Excursion",
                 "Excursion", "Vendeur", "Status", "Etat_Excursion"],
        inplace=True, errors="ignore"
    )
    before = len(fact_excursion_final)
    fact_excursion_final.drop_duplicates(subset=["Num_Ticket"], keep="first", inplace=True)
    fact_excursion_final.reset_index(drop=True, inplace=True)
    if before > len(fact_excursion_final):
        log(f"  ⚠️  fact_excursion : {before - len(fact_excursion_final):,} doublons Num_Ticket supprimés.", "warning")
    log(f"  fact_excursion_final : {len(fact_excursion_final):,} lignes.")

    dims = {
        "dim_client":           dim_client,
        "dim_hotel":            dim_hotel,
        "dim_currency":         dim_currency,
        "dim_date_transfert":   dim_date,
        "dim_airport":          dim_airport,
        "dim_flight":           dim_flight,
        "dim_status_transfert": dim_status,
        "dim_date_excursion":   dim_date_exc,
        "dim_excursion":        dim_excursion,
        "dim_vendeur":          dim_vendeur,
        "dim_status_excursion": dim_status_exc,
    }

    return fact_transfert_final, fact_excursion_final, dims

# ================================================================
#  CONTRÔLE QUALITÉ
# ================================================================
def quality_checks(fact_transfert, fact_excursion, dims):
    section("CONTRÔLE QUALITÉ")

    nb_ok   = 0
    nb_fail = 0
    all_ok  = True

    def check(label, result, expected=0):
        nonlocal nb_ok, nb_fail, all_ok
        if result == expected:
            log(f"  ✅ {label} : OK")
            nb_ok += 1
        else:
            log(f"  ❌ {label} : {result:,} anomalie(s)", "warning")
            nb_fail += 1
            all_ok = False

    # ── Doublons PK ───────────────────────────────────────────────
    check("Doublons Num_Reservation",
          fact_transfert.duplicated(subset=["Num_Reservation"]).sum())
    check("Doublons Num_Ticket",
          fact_excursion.duplicated(subset=["Num_Ticket"]).sum())

    # ── Nulls critiques ───────────────────────────────────────────
    for col in ["client_id", "hotel_id", "currency_id", "date_id", "airport_id", "flight_id", "status_id"]:
        if col in fact_transfert.columns:
            check(f"NULL fact_transfert.{col}", fact_transfert[col].isna().sum())
    for col in ["client_id", "hotel_id", "currency_id", "date_id", "excursion_id", "vendeur_id", "status_id"]:
        if col in fact_excursion.columns:
            check(f"NULL fact_excursion.{col}", fact_excursion[col].isna().sum())

    # ── Montants anormaux ─────────────────────────────────────────
    if "Montant" in fact_transfert.columns:
        check("Montant <= 0 fact_transfert",
              (fact_transfert["Montant"] <= 0).sum())
    if "Montant" in fact_excursion.columns:
        check("Montant <= 0 fact_excursion",
              (fact_excursion["Montant"] <= 0).sum())

    # ── Cohérence FK (Python) ─────────────────────────────────────
    fk_checks_t = [
        ("fact_transfert.date_id",     "date_id",     dims["dim_date_transfert"]),
        ("fact_transfert.client_id",   "client_id",   dims["dim_client"]),
        ("fact_transfert.hotel_id",    "hotel_id",    dims["dim_hotel"]),
        ("fact_transfert.currency_id", "currency_id", dims["dim_currency"]),
        ("fact_transfert.airport_id",  "airport_id",  dims["dim_airport"]),
        ("fact_transfert.flight_id",   "flight_id",   dims["dim_flight"]),
        ("fact_transfert.status_id",   "status_id",   dims["dim_status_transfert"]),
    ]
    for label, col, dim_df in fk_checks_t:
        if col in fact_transfert.columns:
            orphans = (~fact_transfert[col].isin(dim_df[col])).sum()
            check(f"Orphelins {label}", orphans)

    fk_checks_e = [
        ("fact_excursion.date_id",      "date_id",      dims["dim_date_excursion"]),
        ("fact_excursion.client_id",    "client_id",    dims["dim_client"]),
        ("fact_excursion.hotel_id",     "hotel_id",     dims["dim_hotel"]),
        ("fact_excursion.currency_id",  "currency_id",  dims["dim_currency"]),
        ("fact_excursion.excursion_id", "excursion_id", dims["dim_excursion"]),
        ("fact_excursion.vendeur_id",   "vendeur_id",   dims["dim_vendeur"]),
        ("fact_excursion.status_id",    "status_id",    dims["dim_status_excursion"]),
    ]
    for label, col, dim_df in fk_checks_e:
        if col in fact_excursion.columns:
            orphans = (~fact_excursion[col].isin(dim_df[col])).sum()
            check(f"Orphelins {label}", orphans)

    log(f"\n  Résultat : {nb_ok}/{nb_ok+nb_fail} checks OK"
        + (" ✅" if all_ok else " ⚠️  anomalies détectées"))

    # Export rapport anomalies si nécessaire
    if "Montant" in fact_transfert.columns:
        anomalies_t = fact_transfert[fact_transfert["Montant"] <= 0]
        if not anomalies_t.empty:
            path = os.path.join(FACT_FINAL_DIR, "anomalies_montant_transfert.csv")
            anomalies_t.to_csv(path, index=False)
            log(f"  📄 Rapport anomalies transfert : {path}")

    if "Montant" in fact_excursion.columns:
        anomalies_e = fact_excursion[fact_excursion["Montant"] <= 0]
        if not anomalies_e.empty:
            path = os.path.join(FACT_FINAL_DIR, "anomalies_montant_excursion.csv")
            anomalies_e.to_csv(path, index=False)
            log(f"  📄 Rapport anomalies excursion : {path}")

    return all_ok

# ================================================================
#  EXPORT FINAL
# ================================================================
def export(fact_transfert, fact_excursion, dims):
    section("EXPORT FINAL")

    # Facts
    fact_transfert.to_csv(os.path.join(FACT_FINAL_DIR, "fact_transfert_final_clean.csv"), index=False)
    fact_excursion.to_csv(os.path.join(FACT_FINAL_DIR, "fact_excursion_final_clean.csv"), index=False)
    log(f"  ✅ fact_transfert_final_clean.csv — {len(fact_transfert):,} lignes")
    log(f"  ✅ fact_excursion_final_clean.csv — {len(fact_excursion):,} lignes")

    # Dimensions
    for name, df in dims.items():
        path = os.path.join(DIM_FINAL_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        log(f"  ✅ {name}.csv — {len(df):,} lignes")

# ================================================================
#  MAIN
# ================================================================
def main():
    run_date     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    global_start = __import__("time").time()
    global_status = "OK"

    section(f"ETL TRANSFORM START — {run_date}")
    log(f"  Log : {_log_file}")

    try:
        extract_transfert()
        extract_excursion()

        fact_transfert, fact_excursion, dims = transform()
        if fact_transfert is None:
            raise RuntimeError("Transformation échouée — vérifiez les fichiers source.")

        qc_ok = quality_checks(fact_transfert, fact_excursion, dims)
        if not qc_ok:
            global_status = "WARNING"

        export(fact_transfert, fact_excursion, dims)

    except Exception as e:
        global_status = "ERROR"
        log(f"\n❌ ERREUR CRITIQUE : {e}", "error")
        traceback.print_exc()

    finally:
        total = __import__("time").time() - global_start
        section(f"ETL TRANSFORM END — {global_status} — {total:.1f}s")
        log(f"  Rapport complet : {_log_file}\n")

if __name__ == "__main__":
    main()