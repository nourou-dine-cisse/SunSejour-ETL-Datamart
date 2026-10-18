"""
===============================================================
  ETL — Datamart Sun Séjour
  Auteur   : Mouhamed Nourou Dine Cisse
  Version  : 5.0 — Production (fix collation DÉFINITIF)
  Phases   : 3 (ETL) + 4 (Contrôle Qualité)
---------------------------------------------------------------
  Modifications vs v4.1 :
    * FIX DÉFINITIF collation : etl_control est supprimé.
      Le suivi des hashs (INSERT/UPDATE) se fait entièrement
      en mémoire Python — aucun JOIN SQL entre tables de
      collations différentes (Latin1_General_CI_AS / Turkish_CI_AS).
      Seuls les UPDATEs métier passent encore par une table
      temp (##upd_<table>) avec COLLATE DATABASE_DEFAULT sur
      le JOIN de PK, ce qui suffit.
    * etl_run_log et etl_checkpoint conservés (pas de JOIN
      inter-collation pas de problème).
===============================================================
"""

import pandas as pd
import pymssql
import numpy as np
import os
import hashlib
import time
import logging
import traceback
from datetime import datetime

# ================================================================
#  CONFIGURATION
# ================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR  = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
from config import DB_CONFIG
DB_CONFIG = DB_CONFIG

DIM_DIR  = os.path.join(BASE_DIR, "etl_ready", "dim_final")
FACT_DIR = os.path.join(BASE_DIR, "etl_ready", "fact_final")

DIMS = [
    ("dim_date_transfert.csv",   "dim_date_transfert",  "date_id"),
    ("dim_client.csv",           "dim_client",          "client_id"),
    ("dim_hotel.csv",            "dim_hotel",           "hotel_id"),
    ("dim_flight.csv",           "dim_flight",          "flight_id"),
    ("dim_currency.csv",         "dim_currency",        "currency_id"),
    ("dim_status_transfert.csv", "dim_status_transfert","status_id"),
    ("dim_airport.csv",          "dim_airport",         "airport_id"),
    ("dim_date_excursion.csv",   "dim_date_excursion",  "date_id"),
    ("dim_excursion.csv",        "dim_excursion",       "excursion_id"),
    ("dim_vendeur.csv",          "dim_vendeur",         "vendeur_id"),
    ("dim_status_excursion.csv", "dim_status_excursion","status_id"),
]

FACTS = [
    ("fact_transfert_final_clean.csv", "fact_transfert", "Num_Reservation"),
    ("fact_excursion_final_clean.csv", "fact_excursion", "Num_Ticket"),
]

FK_CONSTRAINTS = {
    "fact_transfert": {
        "FK_ft_date":     ("date_id",    "dim_date_transfert"),
        "FK_ft_client":   ("client_id",  "dim_client"),
        "FK_ft_hotel":    ("hotel_id",   "dim_hotel"),
        "FK_ft_flight":   ("flight_id",  "dim_flight"),
        "FK_ft_currency": ("currency_id","dim_currency"),
        "FK_ft_status":   ("status_id",  "dim_status_transfert"),
        "FK_ft_airport":  ("airport_id", "dim_airport"),
    },
    "fact_excursion": {
        "FK_fe_date":      ("date_id",      "dim_date_excursion"),
        "FK_fe_client":    ("client_id",    "dim_client"),
        "FK_fe_hotel":     ("hotel_id",     "dim_hotel"),
        "FK_fe_currency":  ("currency_id",  "dim_currency"),
        "FK_fe_excursion": ("excursion_id", "dim_excursion"),
        "FK_fe_vendeur":   ("vendeur_id",   "dim_vendeur"),
        "FK_fe_status":    ("status_id",    "dim_status_excursion"),
    },
}

CHUNK_SIZE = 2000

# ================================================================
#  LOGGING
# ================================================================
_log_file = os.path.join(LOG_DIR, f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
_logger = logging.getLogger("ETL")

def log(msg, level="info"):
    getattr(_logger, level)(msg)

def section(title):
    log("=" * 60)
    log(f"  {title}")
    log("=" * 60)

# ================================================================
#  CONNEXION
# ================================================================
def get_connection(db_name="master"):
    conf = DB_CONFIG.copy()
    conf["database"] = db_name
    return pymssql.connect(**conf, autocommit=True)

# ================================================================
#  TYPAGE SQL
# ================================================================
def sql_type_mapper(dtype, col_name=None, pk=None):
    if "int"   in str(dtype): return "BIGINT"
    if "float" in str(dtype): return "FLOAT"
    if col_name and pk and col_name == pk:
        return "NVARCHAR(100)"
    return "NVARCHAR(255)"

# ================================================================
#  NETTOYAGE DATAFRAME
# ================================================================
def clean_df(df):
    df = df.loc[:, df.columns.notna()]
    df.columns = df.columns.astype(str).str.strip()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip().replace(
                {"nan": None, "None": None, "": None}
            )
        else:
            df[col] = df[col].where(pd.notna(df[col]), other=None)
    return df

# ================================================================
#  HASH MD5 STABLE — EN MÉMOIRE UNIQUEMENT, JAMAIS EN BASE MÉTIER
# ================================================================
def compute_row_hash(df, non_pk_cols):
    def row_hash(row):
        raw = "|".join(
            "" if (v is None or str(v) in ("nan", "None")) else str(v).strip()
            for v in row
        )
        return hashlib.md5(raw.encode("utf-8")).hexdigest()
    return df[non_pk_cols].apply(row_hash, axis=1)

# ================================================================
#  TABLES TECHNIQUES ETL
# ================================================================
def create_etl_tables(cursor):
    # etl_control SUPPRIMÉ (v5.0) — suivi des hashs en mémoire Python.
    # Si une ancienne version a créé la table, on la supprime proprement.
    cursor.execute("IF OBJECT_ID('etl_control', 'U') IS NOT NULL DROP TABLE etl_control")

    cursor.execute("""
        IF OBJECT_ID('etl_run_log', 'U') IS NULL
        CREATE TABLE etl_run_log (
            run_id       INT IDENTITY(1,1) PRIMARY KEY,
            run_date     DATETIME      NOT NULL DEFAULT GETDATE(),
            table_name   NVARCHAR(100) NOT NULL,
            nb_insert    INT           NOT NULL DEFAULT 0,
            nb_update    INT           NOT NULL DEFAULT 0,
            nb_delete    INT           NOT NULL DEFAULT 0,
            nb_unchanged INT           NOT NULL DEFAULT 0,
            duration_sec FLOAT         NOT NULL DEFAULT 0,
            status       NVARCHAR(10)  NOT NULL DEFAULT 'OK',
            error_msg    NVARCHAR(MAX) NULL
        )
    """)
    cursor.execute("""
        IF OBJECT_ID('etl_checkpoint', 'U') IS NULL
        CREATE TABLE etl_checkpoint (
            run_date   DATETIME      NOT NULL,
            table_name NVARCHAR(100) NOT NULL,
            status     NVARCHAR(20)  NOT NULL,
            CONSTRAINT PK_etl_checkpoint PRIMARY KEY (run_date, table_name)
        )
    """)

# ── etl_control supprimé (v5.0) : toutes les fonctions de suivi
#    des hashs fonctionnent désormais EN MÉMOIRE PYTHON.
#    Plus aucun JOIN SQL entre tables de collations différentes.

def load_control_from_db(cursor, table, pk, df_cols):
    """
    Relit toute la table métier depuis SQL Server et recalcule
    les hashs en Python. Retourne {pk_value: row_hash}.
    Appelé une seule fois par table au démarrage du _sync.
    """
    cursor.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()
    if not rows:
        return {}
    cols_db = [desc[0] for desc in cursor.description]
    df_db   = clean_df(pd.DataFrame(rows, columns=cols_db))
    non_pk  = [c for c in df_db.columns if c != pk]
    df_db["_h"] = compute_row_hash(df_db, non_pk)
    return {str(row[pk]).strip(): row["_h"] for _, row in df_db.iterrows()}

# ── etl_run_log ──────────────────────────────────────────────────
def log_run(cursor, table, nb_i, nb_u, nb_d, nb_s, duration, status="OK", error=None):
    cursor.execute("""
        INSERT INTO etl_run_log
            (table_name, nb_insert, nb_update, nb_delete, nb_unchanged,
             duration_sec, status, error_msg)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (table, nb_i, nb_u, nb_d, nb_s, round(duration, 2), status, error))

# ── etl_checkpoint ───────────────────────────────────────────────
def checkpoint_done(cursor, run_date, table):
    cursor.execute(
        "SELECT 1 FROM etl_checkpoint WHERE run_date=%s AND table_name=%s AND status='DONE'",
        (run_date, table)
    )
    return cursor.fetchone() is not None

def checkpoint_set(cursor, run_date, table, status="DONE"):
    cursor.execute("""
        MERGE etl_checkpoint AS t
        USING (SELECT %s AS run_date, %s AS table_name, %s AS status) AS s
        ON t.run_date=s.run_date AND t.table_name=s.table_name
        WHEN MATCHED THEN UPDATE SET status=s.status
        WHEN NOT MATCHED THEN INSERT (run_date,table_name,status)
                              VALUES (s.run_date,s.table_name,s.status);
    """, (run_date, table, status))

def checkpoint_clear(cursor, run_date):
    cursor.execute("DELETE FROM etl_checkpoint WHERE run_date=%s", (run_date,))

# ================================================================
#  CRÉATION TABLE MÉTIER — 100% PROPRE, ZÉRO COLONNE TECHNIQUE
# ================================================================
def create_table(cursor, table, pk, df):
    # Colonnes métier uniquement — pas de _row_hash ici
    columns_def = ", ".join([
        f"[{c}] {sql_type_mapper(d, col_name=c, pk=pk)}"
        for c, d in zip(df.columns, df.dtypes)
    ])
    try:
        cursor.execute(f"""
            IF OBJECT_ID('{table}', 'U') IS NULL
            CREATE TABLE {table} (
                {columns_def},
                CONSTRAINT PK_{table} PRIMARY KEY ([{pk}])
            )
        """)
        cursor.execute(f"SELECT OBJECT_ID('{table}', 'U')")
        if cursor.fetchone()[0]:
            log(f"  [{table}] ✅ Table prête.")
            return True
        log(f"  [{table}] ❌ Création échouée.", "error")
        return False
    except Exception as e:
        log(f"  [{table}] ❌ Erreur création : {e}", "error")
        return False

# ================================================================
#  BULK UPDATE VIA TABLE TEMPORAIRE (anti-timeout)
#  FIX v4.1 : COLLATE DATABASE_DEFAULT sur toutes les colonnes NVARCHAR
#  de la temp table pour éviter le conflit de collation entre la base
#  et tempdb sur certains serveurs (ex: Turkish_CI_AS).
# ================================================================
def bulk_update_via_temp(cursor, table, pk, cols, non_pk_cols, to_update):
    tmp      = f"##upd_{table}"
    col_defs = ", ".join([f"[{c}] NVARCHAR(500) COLLATE DATABASE_DEFAULT" for c in cols])
    col_sql  = ", ".join([f"[{c}]" for c in cols])
    ph       = ", ".join(["%s"] * len(cols))
    cursor.execute(f"IF OBJECT_ID('tempdb..{tmp}') IS NOT NULL DROP TABLE {tmp}")
    cursor.execute(f"CREATE TABLE {tmp} ({col_defs})")
    rows = pd.DataFrame(to_update)[cols].values.tolist()
    for i in range(0, len(rows), CHUNK_SIZE):
        cursor.executemany(f"INSERT INTO {tmp} ({col_sql}) VALUES ({ph})", rows[i:i+CHUNK_SIZE])
    set_clause = ", ".join([f"t.[{c}]=s.[{c}]" for c in non_pk_cols])
    cursor.execute(f"""
        UPDATE t SET {set_clause}
        FROM {table} t INNER JOIN {tmp} s ON t.[{pk}]=s.[{pk}]
    """)
    cursor.execute(f"DROP TABLE {tmp}")

# ================================================================
#  SYNC COMMUNE
# ================================================================
def _sync(cursor, df, table, pk, allow_delete, run_date):
    if df.empty:
        log(f"  [{table}] ⚠️  DataFrame vide, skip.", "warning")
        return
    if checkpoint_done(cursor, run_date, table):
        log(f"  [{table}] ⏭️  Checkpoint OK, skip.")
        return

    t0 = time.time()
    df = clean_df(df)
    before = len(df)
    df = df.drop_duplicates(subset=[pk], keep="first")
    if len(df) < before:
        log(f"  [{table}] ⚠️  {before-len(df):,} doublons CSV supprimés.", "warning")

    cols        = list(df.columns)
    non_pk_cols = [c for c in cols if c != pk]
    df          = df.copy()
    df["_hash"] = compute_row_hash(df, non_pk_cols)

    # v5.0 : lecture de l'état courant de la table métier en mémoire Python.
    # Aucun JOIN SQL inter-collation — 0 risque de conflit.
    log(f"  [{table}] 🔍 Lecture état courant depuis SQL Server...")
    control = load_control_from_db(cursor, table, pk, cols)
    log(f"  [{table}] 📋 {len(control):,} lignes existantes en mémoire.")

    to_insert    = []
    to_update    = []

    for _, row in df.iterrows():
        pk_val   = str(row[pk]).strip()
        new_hash = row["_hash"]
        if pk_val not in control:
            to_insert.append(row)
        elif control[pk_val] != new_hash:
            to_update.append(row)

    pk_csv      = set(df[pk].astype(str).str.strip())
    to_delete   = list(set(control.keys()) - pk_csv) if allow_delete else []
    disappeared = [] if allow_delete else list(set(control.keys()) - pk_csv)

    skipped = len(df) - len(to_insert) - len(to_update)
    log(f"  [{table}] +{len(to_insert):,} insert | ~{len(to_update):,} update | "
        f"🗑️  {len(to_delete):,} delete | ={skipped:,} inchangés")

    col_sql = ", ".join([f"[{c}]" for c in cols])
    ph      = ", ".join(["%s"] * len(cols))

    try:
        if to_insert:
            rows = pd.DataFrame(to_insert)[cols].values.tolist()
            for i in range(0, len(rows), CHUNK_SIZE):
                cursor.executemany(f"INSERT INTO {table} ({col_sql}) VALUES ({ph})", rows[i:i+CHUNK_SIZE])
        if to_update:
            bulk_update_via_temp(cursor, table, pk, cols, non_pk_cols, to_update)
        if to_delete:
            for i in range(0, len(to_delete), CHUNK_SIZE):
                chunk = to_delete[i:i+CHUNK_SIZE]
                ph_d  = ", ".join(["%s"] * len(chunk))
                cursor.execute(f"DELETE FROM {table} WHERE [{pk}] IN ({ph_d})", chunk)

        # v5.0 : pas de save_control / delete_control — tout est en mémoire.
        duration = time.time() - t0
        log_run(cursor, table, len(to_insert), len(to_update), len(to_delete), skipped, duration, "OK")
        checkpoint_set(cursor, run_date, table, "DONE")
        log(f"  [{table}] ✅ Sync terminée en {duration:.1f}s")

    except Exception as e:
        duration = time.time() - t0
        log_run(cursor, table, len(to_insert), len(to_update), len(to_delete), skipped, duration, "ERROR", str(e))
        checkpoint_set(cursor, run_date, table, "ERROR")
        log(f"  [{table}] ❌ Erreur : {e}", "error")
        raise

def sync_dim(cursor, df, table, pk, run_date):
    _sync(cursor, df, table, pk, allow_delete=False, run_date=run_date)

def sync_fact(cursor, df, table, pk, run_date):
    _sync(cursor, df, table, pk, allow_delete=True, run_date=run_date)

# ================================================================
#  CLÉS ÉTRANGÈRES
# ================================================================
def add_fk(cursor, fact_table, fk_name, col, ref_table):
    try:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name='{fk_name}')
            ALTER TABLE {fact_table}
            ADD CONSTRAINT {fk_name}
            FOREIGN KEY ([{col}]) REFERENCES {ref_table}([{col}])
        """)
        log(f"  [{fact_table}] FK {fk_name} ✅")
    except Exception as e:
        log(f"  [{fact_table}] FK {fk_name} ⚠️ : {e}", "warning")

# ================================================================
#  VUES ANALYTIQUES CA + TABLE JOINTE
# ================================================================
def create_analytics(cursor):
    section("ANALYTIQUES CA — VUES & TABLE JOINTE")

    analytics = {

        # ── Table jointe transfert+excursion sur client/date/hotel ─
        # Recréée à chaque run (TRUNCATE + INSERT)
        "fact_joint_creation": """
            IF OBJECT_ID('fact_joint', 'U') IS NULL
            CREATE TABLE fact_joint (
                transfert_reservation  NVARCHAR(100) NOT NULL,
                excursion_reservation  NVARCHAR(100) NOT NULL,
                client_id              BIGINT,
                hotel_id               BIGINT,
                Date_Transfert         NVARCHAR(50),
                Date_Arrivee           NVARCHAR(50),
                CONSTRAINT PK_fact_joint PRIMARY KEY (transfert_reservation, excursion_reservation)
            )
        """
    }

    for name, sql in analytics.items():
        try:
            cursor.execute(sql.strip())
            log(f"  {name} ✅")
        except Exception as e:
            log(f"  {name} ⚠️ : {e}", "warning")

    # ── Alimentation fact_joint (TRUNCATE + INSERT) ───────────────────────────
    # Jointure transfert ↔ excursion sur client_id + hotel_id
    # avec condition de date : Date_Transfert = Date_Arrivee excursion
    try:
        cursor.execute("TRUNCATE TABLE fact_joint")
        cursor.execute("""
            INSERT INTO fact_joint
                (transfert_reservation, excursion_reservation,
                 client_id, hotel_id, Date_Transfert, Date_Arrivee)
            SELECT
                CAST(ft.Num_Reservation AS NVARCHAR(100)) AS transfert_reservation,
                CAST(fe.Num_Reservation AS NVARCHAR(100)) AS excursion_reservation,
                ft.client_id,
                ft.hotel_id,
                CAST(dt.[Date]        AS NVARCHAR(50))  AS Date_Transfert,
                CAST(de.Date_Arrivee  AS NVARCHAR(50))  AS Date_Arrivee
            FROM fact_transfert ft
            JOIN dim_date_transfert  dt ON dt.date_id  = ft.date_id
            JOIN fact_excursion      fe ON fe.client_id = ft.client_id
                                      AND fe.hotel_id  = ft.hotel_id
            JOIN dim_date_excursion  de ON de.date_id  = fe.date_id
            WHERE dt.[Date] = CONVERT(date, de.Date_Arrivee, 103)
        """)
        cursor.execute("SELECT COUNT(*) FROM fact_joint")
        n = cursor.fetchone()[0]
        log(f"  fact_joint ✅ — {n:,} lignes (transfert ↔ excursion / client+hotel+date)")
    except Exception as e:
        log(f"  fact_joint ⚠️ : {e}", "warning")

# ================================================================
#  CONTRÔLE QUALITÉ
# ================================================================
def run_quality_checks(cursor):
    section("CONTRÔLE QUALITÉ")

    checks = [
        ("Orphelins fact_transfert.date_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_date_transfert d WHERE d.date_id=ft.date_id)"),
        ("Orphelins fact_transfert.client_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_client d WHERE d.client_id=ft.client_id)"),
        ("Orphelins fact_transfert.hotel_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_hotel d WHERE d.hotel_id=ft.hotel_id)"),
        ("Orphelins fact_transfert.currency_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_currency d WHERE d.currency_id=ft.currency_id)"),
        ("Orphelins fact_transfert.airport_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_airport d WHERE d.airport_id=ft.airport_id)"),
        ("Orphelins fact_transfert.flight_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_flight d WHERE d.flight_id=ft.flight_id)"),
        ("Orphelins fact_transfert.status_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_status_transfert d WHERE d.status_id=ft.status_id)"),
        ("Orphelins fact_excursion.date_id",
         "SELECT COUNT(*) FROM fact_excursion fe WHERE NOT EXISTS (SELECT 1 FROM dim_date_excursion d WHERE d.date_id=fe.date_id)"),
        ("Orphelins fact_excursion.client_id",
         "SELECT COUNT(*) FROM fact_excursion fe WHERE NOT EXISTS (SELECT 1 FROM dim_client d WHERE d.client_id=fe.client_id)"),
        ("Orphelins fact_excursion.hotel_id",
         "SELECT COUNT(*) FROM fact_excursion fe WHERE NOT EXISTS (SELECT 1 FROM dim_hotel d WHERE d.hotel_id=fe.hotel_id)"),
        ("Orphelins fact_excursion.excursion_id",
         "SELECT COUNT(*) FROM fact_excursion fe WHERE NOT EXISTS (SELECT 1 FROM dim_excursion d WHERE d.excursion_id=fe.excursion_id)"),
        ("Orphelins fact_excursion.vendeur_id",
         "SELECT COUNT(*) FROM fact_excursion fe WHERE NOT EXISTS (SELECT 1 FROM dim_vendeur d WHERE d.vendeur_id=fe.vendeur_id)"),
        ("Orphelins fact_excursion.status_id",
         "SELECT COUNT(*) FROM fact_excursion fe WHERE NOT EXISTS (SELECT 1 FROM dim_status_excursion d WHERE d.status_id=fe.status_id)"),
        ("NULL fact_transfert.Montant",   "SELECT COUNT(*) FROM fact_transfert WHERE Montant IS NULL"),
        ("NULL fact_transfert.client_id", "SELECT COUNT(*) FROM fact_transfert WHERE client_id IS NULL"),
        ("NULL fact_excursion.Montant",   "SELECT COUNT(*) FROM fact_excursion WHERE Montant IS NULL"),
        ("NULL fact_excursion.vendeur_id","SELECT COUNT(*) FROM fact_excursion WHERE vendeur_id IS NULL"),
        ("Montant <= 0 fact_transfert",   "SELECT COUNT(*) FROM fact_transfert WHERE Montant <= 0"),
        ("Montant <= 0 fact_excursion",   "SELECT COUNT(*) FROM fact_excursion WHERE Montant <= 0"),
        ("Doublons Num_Reservation",
         "SELECT COUNT(*) FROM (SELECT Num_Reservation FROM fact_transfert GROUP BY Num_Reservation HAVING COUNT(*)>1) x"),
        ("Doublons Num_Ticket",
         "SELECT COUNT(*) FROM (SELECT Num_Ticket FROM fact_excursion GROUP BY Num_Ticket HAVING COUNT(*)>1) x"),
    ]

    all_ok, nb_ok, nb_fail = True, 0, 0
    for label, sql in checks:
        try:
            cursor.execute(sql)
            count = cursor.fetchone()[0]
            if count == 0:
                log(f"  ✅ {label} : OK"); nb_ok += 1
            else:
                log(f"  ❌ {label} : {count:,} anomalie(s)", "warning"); nb_fail += 1; all_ok = False
        except Exception as e:
            log(f"  ⚠️  {label} : {e}", "warning"); nb_fail += 1; all_ok = False

    log(f"\n  Résultat : {nb_ok}/{nb_ok+nb_fail} checks OK" + (" ✅" if all_ok else " ⚠️  anomalies détectées"))

    log("\n  --- Volumes chargés ---")
    for t in ["fact_transfert", "fact_excursion", "fact_joint"] + [d[1] for d in DIMS]:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {t}")
            log(f"  {t:<35} {cursor.fetchone()[0]:>10,} lignes")
        except:
            pass

    return all_ok

# ================================================================
#  MAIN
# ================================================================
def main():
    run_date      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    global_start  = time.time()
    global_status = "OK"

    section(f"ETL START — {run_date}")
    log(f"  Log : {_log_file}")

    conn_master = get_connection("master")
    cur = conn_master.cursor()
    cur.execute(f"IF DB_ID('{DB_CONFIG['database']}') IS NULL CREATE DATABASE {DB_CONFIG['database']}")
    conn_master.close()
    log("✅ Base Datamart prête.")

    conn   = get_connection(DB_CONFIG["database"])
    cursor = conn.cursor()

    try:
        create_etl_tables(cursor)

        # 1. DIMENSIONS
        section("DIMENSIONS")
        for file, table, pk in DIMS:
            path = os.path.join(DIM_DIR, file)
            if not os.path.exists(path):
                log(f"  [{table}] ⚠️  Introuvable : {path}", "warning"); continue
            df = pd.read_csv(path)
            if create_table(cursor, table, pk, df):
                sync_dim(cursor, df, table, pk, run_date)

        # 2. TABLES DE FAITS — création
        section("TABLES DE FAITS — création")
        fact_dfs = {}
        for file, table, pk in FACTS:
            path = os.path.join(FACT_DIR, file)
            if not os.path.exists(path):
                log(f"  [{table}] ⚠️  Introuvable : {path}", "warning"); continue
            df = pd.read_csv(path)
            fact_dfs[(table, pk)] = df
            create_table(cursor, table, pk, df)

        # 3. CLÉS ÉTRANGÈRES
        section("CLÉS ÉTRANGÈRES")
        for fact_table, fks in FK_CONSTRAINTS.items():
            for fk_name, (col, ref_table) in fks.items():
                add_fk(cursor, fact_table, fk_name, col, ref_table)

        # 4. SYNC DES FAITS
        section("TABLES DE FAITS — synchronisation")
        for _, table, pk in FACTS:
            if (table, pk) in fact_dfs:
                sync_fact(cursor, fact_dfs[(table, pk)], table, pk, run_date)

        # 5. ANALYTIQUES CA (vues + fact_joint)
        create_analytics(cursor)

        # 6. CONTRÔLE QUALITÉ
        qc_ok = run_quality_checks(cursor)
        if not qc_ok:
            global_status = "WARNING"

        checkpoint_clear(cursor, run_date)

    except Exception as e:
        global_status = "ERROR"
        log(f"\n❌ ERREUR CRITIQUE : {e}", "error")
        log("⚠️  Checkpoints conservés — relancez pour reprendre.", "warning")
        traceback.print_exc()

    finally:
        cursor.close()
        conn.close()
        total = time.time() - global_start
        section(f"ETL END — {global_status} — {total:.1f}s")
        log(f"  Rapport complet : {_log_file}\n")


if __name__ == "__main__":
    main()