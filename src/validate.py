"""
===============================================================
  VALIDATION — Datamart Sun Séjour
  Auteur  : Mouhamed Nourou Dine Cisse
  Version : 1.0 — Production
---------------------------------------------------------------
  3 modes d'utilisation :

    python validate.py --before   # snapshot avant run ETL
    python validate.py --after    # snapshot après run ETL + rapport
    python validate.py --check    # vérification cohérence seule

  Le snapshot "before" est sauvegardé dans logs/snapshot_before.json
  Le rapport de comparaison est écrit dans logs/validation_YYYYMMDD.log
===============================================================
"""

import pymssql
import json
import os
import sys
import logging
from datetime import datetime
from config import DB_CONFIG

# ================================================================
#  CONFIGURATION
# ================================================================
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
LOG_DIR      = os.path.join(BASE_DIR, "logs")
SNAPSHOT_FILE = os.path.join(LOG_DIR, "snapshot_before.json")
os.makedirs(LOG_DIR, exist_ok=True)

_log_file = os.path.join(LOG_DIR, f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("VALIDATE").info
warn = logging.getLogger("VALIDATE").warning
err  = logging.getLogger("VALIDATE").error

def section(title):
    log("=" * 60)
    log(f"  {title}")
    log("=" * 60)

# ================================================================
#  CONNEXION
# ================================================================
def get_cursor():
    conn = pymssql.connect(**DB_CONFIG, autocommit=True)
    return conn, conn.cursor()

# ================================================================
#  SNAPSHOT — photographie de l'état de la base
# ================================================================
TABLES_METIER = [
    "fact_transfert", "fact_excursion", "fact_joint",
    "fact_ca_semaine",
    "dim_client", "dim_hotel", "dim_currency",
    "dim_date_transfert", "dim_date_excursion",
    "dim_flight", "dim_excursion", "dim_vendeur",
    "dim_airport", "dim_status_transfert", "dim_status_excursion",
]

TABLES_TECH = ["etl_control", "etl_run_log", "etl_checkpoint"]

def take_snapshot(cursor):
    snapshot = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "volumes": {},
        "ca_par_annee": {},
        "montants": {},
        "derniers_runs": [],
    }

    # ── Volumes par table ────────────────────────────────────────
    for table in TABLES_METIER + TABLES_TECH:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            snapshot["volumes"][table] = cursor.fetchone()[0]
        except:
            snapshot["volumes"][table] = None

    # ── CA par année ─────────────────────────────────────────────
    try:
        cursor.execute("""
            SELECT annee,
                CAST(ROUND(SUM(ca_transfert), 2) AS FLOAT) AS ca_t,
                CAST(ROUND(SUM(ca_excursion), 2) AS FLOAT) AS ca_e,
                CAST(ROUND(SUM(ca_total),     2) AS FLOAT) AS ca_tot
            FROM fact_ca_semaine
            GROUP BY annee ORDER BY annee
        """)
        for r in cursor.fetchall():
            snapshot["ca_par_annee"][str(r[0])] = {
                "ca_transfert": float(r[1]),
                "ca_excursion": float(r[2]),
                "ca_total":     float(r[3]),
            }
    except:
        pass

    # ── Montants totaux dans les faits ───────────────────────────
    for table, col in [("fact_transfert", "Montant"), ("fact_excursion", "Montant")]:
        try:
            cursor.execute(f"""
                SELECT
                    CAST(ROUND(SUM({col}), 2) AS FLOAT),
                    CAST(ROUND(AVG({col}), 2) AS FLOAT),
                    CAST(ROUND(MIN({col}), 2) AS FLOAT),
                    CAST(ROUND(MAX({col}), 2) AS FLOAT),
                    SUM(CASE WHEN {col} <= 0 THEN 1 ELSE 0 END)
                FROM {table}
            """)
            r = cursor.fetchone()
            snapshot["montants"][table] = {
                "sum": float(r[0]) if r[0] else 0,
                "avg": float(r[1]) if r[1] else 0,
                "min": float(r[2]) if r[2] else 0,
                "max": float(r[3]) if r[3] else 0,
                "negatifs": int(r[4]) if r[4] else 0,
            }
        except:
            pass

    # ── 5 derniers runs ETL ──────────────────────────────────────
    try:
        cursor.execute("""
            SELECT TOP 5 run_date, table_name, nb_insert, nb_update,
                         nb_delete, nb_unchanged, duration_sec, status
            FROM etl_run_log
            ORDER BY run_id DESC
        """)
        for r in cursor.fetchall():
            snapshot["derniers_runs"].append({
                "run_date":    str(r[0]),
                "table":       r[1],
                "insert":      r[2],
                "update":      r[3],
                "delete":      r[4],
                "unchanged":   r[5],
                "duration":    float(r[6]),
                "status":      r[7],
            })
    except:
        pass

    return snapshot

# ================================================================
#  CHECKS DE COHÉRENCE
# ================================================================
def run_coherence_checks(cursor):
    section("COHÉRENCE DU DATAMART")

    checks_ok = 0
    checks_fail = 0

    def ok(label):
        nonlocal checks_ok
        log(f"  ✅ {label}")
        checks_ok += 1

    def ko(label, detail=""):
        nonlocal checks_fail
        warn(f"  ❌ {label}" + (f" — {detail}" if detail else ""))
        checks_fail += 1

    def check_zero(label, sql):
        cursor.execute(sql)
        n = cursor.fetchone()[0]
        if n == 0:
            ok(label)
        else:
            ko(label, f"{n:,} anomalie(s)")

    # ── Intégrité référentielle ───────────────────────────────────
    section("1. Intégrité référentielle (FK)")
    fk_checks = [
        ("Orphelins fact_transfert.date_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_date_transfert d WHERE d.date_id=ft.date_id)"),
        ("Orphelins fact_transfert.client_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_client d WHERE d.client_id=ft.client_id)"),
        ("Orphelins fact_transfert.hotel_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_hotel d WHERE d.hotel_id=ft.hotel_id)"),
        ("Orphelins fact_transfert.currency_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_currency d WHERE d.currency_id=ft.currency_id)"),
        ("Orphelins fact_transfert.flight_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_flight d WHERE d.flight_id=ft.flight_id)"),
        ("Orphelins fact_transfert.airport_id",
         "SELECT COUNT(*) FROM fact_transfert ft WHERE NOT EXISTS (SELECT 1 FROM dim_airport d WHERE d.airport_id=ft.airport_id)"),
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
    ]
    for label, sql in fk_checks:
        check_zero(label, sql)

    # ── Doublons PK ───────────────────────────────────────────────
    section("2. Unicité des clés primaires")
    pk_checks = [
        ("Doublons fact_transfert.Num_Reservation",
         "SELECT COUNT(*) FROM (SELECT Num_Reservation FROM fact_transfert GROUP BY Num_Reservation HAVING COUNT(*)>1) x"),
        ("Doublons fact_excursion.Num_Ticket",
         "SELECT COUNT(*) FROM (SELECT Num_Ticket FROM fact_excursion GROUP BY Num_Ticket HAVING COUNT(*)>1) x"),
        ("Doublons dim_client.client_id",
         "SELECT COUNT(*) FROM (SELECT client_id FROM dim_client GROUP BY client_id HAVING COUNT(*)>1) x"),
        ("Doublons dim_hotel.hotel_id",
         "SELECT COUNT(*) FROM (SELECT hotel_id FROM dim_hotel GROUP BY hotel_id HAVING COUNT(*)>1) x"),
        ("Doublons dim_date_transfert.date_id",
         "SELECT COUNT(*) FROM (SELECT date_id FROM dim_date_transfert GROUP BY date_id HAVING COUNT(*)>1) x"),
        ("Doublons dim_date_excursion.date_id",
         "SELECT COUNT(*) FROM (SELECT date_id FROM dim_date_excursion GROUP BY date_id HAVING COUNT(*)>1) x"),
    ]
    for label, sql in pk_checks:
        check_zero(label, sql)

    # ── Nulls critiques ───────────────────────────────────────────
    section("3. Valeurs NULL critiques")
    null_checks = [
        ("NULL fact_transfert.Montant",    "SELECT COUNT(*) FROM fact_transfert WHERE Montant IS NULL"),
        ("NULL fact_transfert.client_id",  "SELECT COUNT(*) FROM fact_transfert WHERE client_id IS NULL"),
        ("NULL fact_transfert.hotel_id",   "SELECT COUNT(*) FROM fact_transfert WHERE hotel_id IS NULL"),
        ("NULL fact_excursion.Montant",    "SELECT COUNT(*) FROM fact_excursion WHERE Montant IS NULL"),
        ("NULL fact_excursion.client_id",  "SELECT COUNT(*) FROM fact_excursion WHERE client_id IS NULL"),
        ("NULL fact_excursion.hotel_id",   "SELECT COUNT(*) FROM fact_excursion WHERE hotel_id IS NULL"),
        ("NULL fact_excursion.vendeur_id", "SELECT COUNT(*) FROM fact_excursion WHERE vendeur_id IS NULL"),
    ]
    for label, sql in null_checks:
        check_zero(label, sql)

    # ── Cohérence CA ─────────────────────────────────────────────
    section("4. Cohérence tables CA")

    # ca_total = ca_transfert + ca_excursion
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM fact_ca_semaine
            WHERE ABS(ca_total - (ca_transfert + ca_excursion)) > 0.01
        """)
        n = cursor.fetchone()[0]
        if n == 0:
            ok("fact_ca_semaine : ca_total = ca_transfert + ca_excursion")
        else:
            ko("fact_ca_semaine : ca_total incohérent", f"{n:,} lignes")
    except Exception as e:
        ko("fact_ca_semaine check", str(e))

    # CA négatifs (warning seulement)
    try:
        cursor.execute("SELECT COUNT(*) FROM fact_ca_semaine WHERE ca_total < 0")
        n = cursor.fetchone()[0]
        if n == 0:
            ok("fact_ca_semaine : aucun ca_total négatif")
        else:
            warn(f"  ⚠️  fact_ca_semaine : {n:,} lignes avec ca_total < 0 (avoirs/remboursements)")
    except:
        pass

    # Cohérence volume : fact_ca_semaine couvre bien les deux sources
    try:
        cursor.execute("SELECT COUNT(DISTINCT client_id) FROM fact_ca_semaine WHERE ca_transfert > 0")
        n_t = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT client_id) FROM fact_ca_semaine WHERE ca_excursion > 0")
        n_e = cursor.fetchone()[0]
        log(f"  ℹ️  Clients avec CA transfert : {n_t:,}")
        log(f"  ℹ️  Clients avec CA excursion : {n_e:,}")
    except:
        pass

    # ── Cohérence CA vs données brutes ─────────────────────────
    section("5. Cohérence CA vs montants bruts (source -> CA)")

    # Règle : SUM(fact_ca_semaine.ca_transfert) doit égaler
    # SUM(fact_transfert.Montant) sur lignes avec client_id+hotel_id valides
    # Tolérance 1.0 : arrondi DECIMAL vs FLOAT
    sum_raw_t, sum_ca_t, sum_raw_e, sum_ca_e = 0.0, 0.0, 0.0, 0.0

    try:
        cursor.execute(
            "SELECT CAST(ROUND(SUM(Montant),2) AS FLOAT) FROM fact_transfert "
            "WHERE Montant IS NOT NULL AND client_id IS NOT NULL AND hotel_id IS NOT NULL"
        )
        sum_raw_t = float(cursor.fetchone()[0] or 0)

        cursor.execute(
            "SELECT CAST(ROUND(SUM(ca_transfert),2) AS FLOAT) FROM fact_ca_semaine"
        )
        sum_ca_t = float(cursor.fetchone()[0] or 0)

        diff_t = abs(sum_raw_t - sum_ca_t)
        log(f"  SUM fact_transfert.Montant  (brut)  : {sum_raw_t:>20,.2f}")
        log(f"  SUM fact_ca_semaine.ca_transfert    : {sum_ca_t:>20,.2f}")
        log(f"  Ecart transfert                     : {diff_t:>20,.2f}")
        if diff_t <= 1.0:
            ok(f"CA transfert coherent avec brut (ecart={diff_t:.2f})")
        else:
            ko("CA transfert diverge du brut", f"ecart={diff_t:,.2f}")
    except Exception as e:
        ko("Check CA transfert vs brut", str(e))

    try:
        cursor.execute(
            "SELECT CAST(ROUND(SUM(Montant),2) AS FLOAT) FROM fact_excursion "
            "WHERE Montant IS NOT NULL AND client_id IS NOT NULL AND hotel_id IS NOT NULL"
        )
        sum_raw_e = float(cursor.fetchone()[0] or 0)

        cursor.execute(
            "SELECT CAST(ROUND(SUM(ca_excursion),2) AS FLOAT) FROM fact_ca_semaine"
        )
        sum_ca_e = float(cursor.fetchone()[0] or 0)

        diff_e = abs(sum_raw_e - sum_ca_e)
        log(f"  SUM fact_excursion.Montant  (brut)  : {sum_raw_e:>20,.2f}")
        log(f"  SUM fact_ca_semaine.ca_excursion    : {sum_ca_e:>20,.2f}")
        log(f"  Ecart excursion                     : {diff_e:>20,.2f}")
        if diff_e <= 1.0:
            ok(f"CA excursion coherent avec brut (ecart={diff_e:.2f})")
        else:
            ko("CA excursion diverge du brut", f"ecart={diff_e:,.2f}")
    except Exception as e:
        ko("Check CA excursion vs brut", str(e))

    try:
        cursor.execute(
            "SELECT CAST(ROUND(SUM(ca_total),2) AS FLOAT) FROM fact_ca_semaine"
        )
        sum_ca_total = float(cursor.fetchone()[0] or 0)
        sum_global   = sum_ca_t + sum_ca_e
        diff_global  = abs(sum_global - sum_ca_total)
        log(f"  SUM ca_t + ca_e (recompute)         : {sum_global:>20,.2f}")
        log(f"  SUM fact_ca_semaine.ca_total        : {sum_ca_total:>20,.2f}")
        log(f"  Ecart global                        : {diff_global:>20,.2f}")
        if diff_global <= 1.0:
            ok(f"CA total coherent (ecart={diff_global:.2f})")
        else:
            ko("CA total incoherent", f"ecart={diff_global:,.2f}")
    except Exception as e:
        ko("Check CA total", str(e))

    # ── Cohérence ETL control ────────────────────────────────────
    section("5. Cohérence ETL control")
    try:
        cursor.execute("""
            SELECT table_name, COUNT(*) AS nb_hashs
            FROM etl_control
            GROUP BY table_name
            ORDER BY table_name
        """)
        rows = cursor.fetchall()
        for r in rows:
            log(f"  ℹ️  etl_control[{r[0]}] : {r[1]:,} hashs")

        # Vérifier que fact_transfert et fact_excursion ont leurs hashs
        tables_with_hash = [r[0] for r in rows]
        for t in ["fact_transfert", "fact_excursion"]:
            if t in tables_with_hash:
                ok(f"etl_control couvert pour {t}")
            else:
                ko(f"etl_control manquant pour {t}")
    except Exception as e:
        ko("Lecture etl_control", str(e))

    # ── Résumé ───────────────────────────────────────────────────
    total = checks_ok + checks_fail
    section(f"RÉSUMÉ : {checks_ok}/{total} checks OK")
    if checks_fail == 0:
        log("  ✅ Tous les checks sont passés")
    else:
        warn(f"  ⚠️  {checks_fail} check(s) en échec — voir détails ci-dessus")

    return checks_fail == 0

# ================================================================
#  RAPPORT AVANT / APRÈS
# ================================================================
def print_comparison(before, after):
    section("RAPPORT AVANT / APRÈS RUN ETL")

    log(f"  Snapshot AVANT : {before['timestamp']}")
    log(f"  Snapshot APRÈS : {after['timestamp']}")
    log("")

    # ── Volumes ──────────────────────────────────────────────────
    section("Volumes par table")
    log(f"  {'Table':<35} {'Avant':>10} {'Après':>10} {'Delta':>10}")
    log("  " + "-" * 68)

    all_tables = sorted(set(list(before["volumes"].keys()) + list(after["volumes"].keys())))
    for table in all_tables:
        b = before["volumes"].get(table)
        a = after["volumes"].get(table)
        if b is None and a is None:
            continue
        b_str = f"{b:,}" if b is not None else "N/A"
        a_str = f"{a:,}" if a is not None else "N/A"
        if b is not None and a is not None:
            delta = a - b
            delta_str = f"+{delta:,}" if delta > 0 else (f"{delta:,}" if delta < 0 else "=")
            marker = " ⬆️" if delta > 0 else (" ⬇️" if delta < 0 else "")
        else:
            delta_str = "N/A"
            marker = ""
        log(f"  {table:<35} {b_str:>10} {a_str:>10} {delta_str:>10}{marker}")

    # ── CA par année ─────────────────────────────────────────────
    if after.get("ca_par_annee"):
        section("CA par année (après run)")
        log(f"  {'Année':<6} {'CA Transfert':>15} {'CA Excursion':>15} {'CA Total':>15}")
        log("  " + "-" * 55)
        for annee, vals in sorted(after["ca_par_annee"].items()):
            log(f"  {annee:<6} {vals['ca_transfert']:>15,.2f} {vals['ca_excursion']:>15,.2f} {vals['ca_total']:>15,.2f}")

    # ── Montants ─────────────────────────────────────────────────
    section("Montants dans les tables de faits")
    for table in ["fact_transfert", "fact_excursion"]:
        b_m = before.get("montants", {}).get(table, {})
        a_m = after.get("montants",  {}).get(table, {})
        if not a_m:
            continue
        log(f"  {table} :")
        log(f"    SUM   : avant={b_m.get('sum',0):>15,.2f}  après={a_m.get('sum',0):>15,.2f}")
        log(f"    AVG   : avant={b_m.get('avg',0):>15,.2f}  après={a_m.get('avg',0):>15,.2f}")
        log(f"    MIN   : avant={b_m.get('min',0):>15,.2f}  après={a_m.get('min',0):>15,.2f}")
        log(f"    MAX   : avant={b_m.get('max',0):>15,.2f}  après={a_m.get('max',0):>15,.2f}")
        log(f"    <=0   : avant={b_m.get('negatifs',0):>10,}       après={a_m.get('negatifs',0):>10,}")

    # ── Derniers runs ────────────────────────────────────────────
    if after.get("derniers_runs"):
        section("5 derniers runs ETL")
        log(f"  {'Date':>20} {'Table':<30} {'INS':>6} {'UPD':>6} {'DEL':>6} {'=':>8} {'s':>6} {'Status'}")
        log("  " + "-" * 95)
        for r in after["derniers_runs"]:
            log(f"  {r['run_date']:>20} {r['table']:<30} {r['insert']:>6,} {r['update']:>6,} {r['delete']:>6,} {r['unchanged']:>8,} {r['duration']:>6.1f} {r['status']}")

# ================================================================
#  MAIN
# ================================================================
def main():
    mode = "--check"
    if len(sys.argv) > 1:
        mode = sys.argv[1]

    conn, cursor = get_cursor()

    try:
        if mode == "--before":
            section(f"SNAPSHOT AVANT — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            snap = take_snapshot(cursor)
            with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
                json.dump(snap, f, ensure_ascii=False, indent=2)
            log(f"  ✅ Snapshot sauvegardé : {SNAPSHOT_FILE}")
            log(f"  Volumes actuels :")
            for t, n in snap["volumes"].items():
                if n is not None:
                    log(f"    {t:<35} {n:>10,} lignes")

        elif mode == "--after":
            section(f"SNAPSHOT APRÈS — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            if not os.path.exists(SNAPSHOT_FILE):
                warn("  ⚠️  Aucun snapshot 'before' trouvé — lance d'abord : python validate.py --before")
                warn("  Lancement du check de cohérence seul...")
                run_coherence_checks(cursor)
            else:
                with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
                    before = json.load(f)
                after = take_snapshot(cursor)
                print_comparison(before, after)
                run_coherence_checks(cursor)
                # Archiver le snapshot after
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                archive = os.path.join(LOG_DIR, f"snapshot_after_{ts}.json")
                with open(archive, "w", encoding="utf-8") as f:
                    json.dump(after, f, ensure_ascii=False, indent=2)
                log(f"\n  Snapshots archivés : {archive}")

        elif mode == "--check":
            section(f"CHECK COHÉRENCE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            run_coherence_checks(cursor)

        else:
            log("Usage :")
            log("  python validate.py --before   # snapshot avant run ETL")
            log("  python validate.py --after    # rapport avant/après + cohérence")
            log("  python validate.py --check    # cohérence seule (défaut)")

    except Exception as e:
        err(f"❌ ERREUR : {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
        log(f"\n  Rapport : {_log_file}")

if __name__ == "__main__":
    main()