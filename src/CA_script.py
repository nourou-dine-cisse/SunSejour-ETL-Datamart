"""
===============================================================
  CA — Datamart Sun Séjour
  Auteur  : Mouhamed Nourou Dine Cisse
  Version : 1.0 — Production
---------------------------------------------------------------
  Calcule et charge fact_ca_semaine :
    client_id + hotel_id + annee + semaine
    ca_transfert / ca_excursion / ca_total

  Re-runnable : DROP + CREATE + INSERT à chaque exécution.

  Dépendances : load.py doit avoir été exécuté avant.
===============================================================
"""

import pymssql
import os
import logging
from datetime import datetime
from config import DB_CONFIG

# ================================================================
#  LOGGING
# ================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR  = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

_log_file = os.path.join(LOG_DIR, f"ca_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("CA").info
err = logging.getLogger("CA").error

def section(title):
    log("=" * 60)
    log(f"  {title}")
    log("=" * 60)

# ================================================================
#  CONNEXION
# ================================================================
def get_connection():
    return pymssql.connect(**DB_CONFIG, autocommit=True)

# ================================================================
#  MAIN
# ================================================================
def main():
    section(f"CA START — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"  Log : {_log_file}")

    conn   = get_connection()
    cursor = conn.cursor()

    try:
        # ── STEP 0 — DROP (re-runnable) ──────────────────────────
        section("STEP 0 — DROP")
        for obj in ["fact_ca_semaine",
                    "tempdb..##ca_t",
                    "tempdb..##ca_e",
                    "tempdb..##ca_combined"]:
            prefix = "TABLE" if "tempdb" not in obj else "TABLE"
            try:
                if "tempdb" in obj:
                    cursor.execute(f"IF OBJECT_ID('{obj}') IS NOT NULL DROP TABLE {obj.split('..')[-1]}")
                else:
                    cursor.execute(f"DROP TABLE IF EXISTS {obj}")
                log(f"  DROP {obj} OK")
            except Exception as e:
                log(f"  DROP {obj} — {e}")

        # ── STEP 1 — CREATE fact_ca_semaine ──────────────────────
        section("STEP 1 — CREATE")
        cursor.execute("""
            CREATE TABLE fact_ca_semaine (
                client_id    BIGINT        NOT NULL,
                hotel_id     BIGINT        NOT NULL,
                annee        INT           NOT NULL,
                semaine      INT           NOT NULL,
                ca_transfert DECIMAL(10,2) NOT NULL DEFAULT 0,
                ca_excursion DECIMAL(10,2) NOT NULL DEFAULT 0,
                ca_total     DECIMAL(10,2) NOT NULL DEFAULT 0,
                CONSTRAINT PK_ca_semaine PRIMARY KEY (client_id, hotel_id, annee, semaine)
            )
        """)
        log("  fact_ca_semaine créée")

        # ── STEP 2 — Source transfert → ##ca_t ───────────────────
        section("STEP 2 — Sources temporaires")

        # Nettoyage sécurisé des tables temporaires
        for tmp in ["##ca_t", "##ca_e", "##ca_combined"]:
            cursor.execute(f"IF OBJECT_ID('tempdb..{tmp}') IS NOT NULL DROP TABLE {tmp}")

        cursor.execute("""
            SELECT
                ft.client_id,
                ft.hotel_id,
                DATEPART(YEAR, TRY_CONVERT(DATE, REPLACE(dt.[Date], ' ', '-'), 23)) AS annee,
                DATEPART(WEEK, TRY_CONVERT(DATE, REPLACE(dt.[Date], ' ', '-'), 23)) AS semaine,
                CAST(ROUND(SUM(ft.Montant), 2) AS DECIMAL(10,2)) AS ca_t
            INTO ##ca_t
            FROM fact_transfert ft
            JOIN dim_date_transfert dt ON dt.date_id = ft.date_id
            WHERE TRY_CONVERT(DATE, REPLACE(dt.[Date], ' ', '-'), 23) IS NOT NULL
              AND ft.client_id IS NOT NULL
              AND ft.hotel_id  IS NOT NULL
            GROUP BY
                ft.client_id, ft.hotel_id,
                DATEPART(YEAR, TRY_CONVERT(DATE, REPLACE(dt.[Date], ' ', '-'), 23)),
                DATEPART(WEEK, TRY_CONVERT(DATE, REPLACE(dt.[Date], ' ', '-'), 23))
        """)
        cursor.execute("SELECT COUNT(*) FROM ##ca_t")
        n = cursor.fetchone()[0]
        log(f"  ##ca_t (transfert) : {n:,} lignes")

        # ── Source excursion → ##ca_e ─────────────────────────────
        # Date de référence : Date_Arrivee (JJ/MM/AAAA → format 103)
        cursor.execute("""
            SELECT
                fe.client_id,
                fe.hotel_id,
                DATEPART(YEAR, TRY_CONVERT(DATE, de.Date_Arrivee, 103)) AS annee,
                DATEPART(WEEK, TRY_CONVERT(DATE, de.Date_Arrivee, 103)) AS semaine,
                CAST(ROUND(SUM(fe.Montant), 2) AS DECIMAL(10,2)) AS ca_e
            INTO ##ca_e
            FROM fact_excursion fe
            JOIN dim_date_excursion de ON de.date_id = fe.date_id
            WHERE TRY_CONVERT(DATE, de.Date_Arrivee, 103) IS NOT NULL
              AND fe.client_id IS NOT NULL
              AND fe.hotel_id  IS NOT NULL
            GROUP BY
                fe.client_id, fe.hotel_id,
                DATEPART(YEAR, TRY_CONVERT(DATE, de.Date_Arrivee, 103)),
                DATEPART(WEEK, TRY_CONVERT(DATE, de.Date_Arrivee, 103))
        """)
        cursor.execute("SELECT COUNT(*) FROM ##ca_e")
        n = cursor.fetchone()[0]
        log(f"  ##ca_e (excursion) : {n:,} lignes")

        # ── STEP 3 — UNION ALL + GROUP BY → ##ca_combined ────────
        section("STEP 3 — Combinaison")
        cursor.execute("""
            SELECT
                client_id, hotel_id, annee, semaine,
                CAST(ROUND(SUM(ca_t), 2) AS DECIMAL(10,2)) AS ca_transfert,
                CAST(ROUND(SUM(ca_e), 2) AS DECIMAL(10,2)) AS ca_excursion,
                CAST(ROUND(SUM(ca_t) + SUM(ca_e), 2) AS DECIMAL(10,2)) AS ca_total
            INTO ##ca_combined
            FROM (
                SELECT client_id, hotel_id, annee, semaine,
                       ca_t, CAST(0.00 AS DECIMAL(10,2)) AS ca_e
                FROM ##ca_t
                UNION ALL
                SELECT client_id, hotel_id, annee, semaine,
                       CAST(0.00 AS DECIMAL(10,2)) AS ca_t, ca_e
                FROM ##ca_e
            ) src
            GROUP BY client_id, hotel_id, annee, semaine
        """)
        cursor.execute("SELECT COUNT(*) FROM ##ca_combined")
        n = cursor.fetchone()[0]
        log(f"  ##ca_combined : {n:,} lignes (client x hotel x annee x semaine)")

        # ── STEP 4 — INSERT → fact_ca_semaine ────────────────────
        section("STEP 4 — Chargement fact_ca_semaine")
        cursor.execute("""
            INSERT INTO fact_ca_semaine
                (client_id, hotel_id, annee, semaine,
                 ca_transfert, ca_excursion, ca_total)
            SELECT client_id, hotel_id, annee, semaine,
                   ca_transfert, ca_excursion, ca_total
            FROM ##ca_combined
        """)
        cursor.execute("SELECT COUNT(*) FROM fact_ca_semaine")
        n = cursor.fetchone()[0]
        log(f"  fact_ca_semaine : {n:,} lignes chargées")

        # ── STEP 5 — Nettoyage ───────────────────────────────────
        section("STEP 5 — Nettoyage")
        for tmp in ["##ca_t", "##ca_e", "##ca_combined"]:
            cursor.execute(f"IF OBJECT_ID('tempdb..{tmp}') IS NOT NULL DROP TABLE {tmp}")
        log("  Tables temporaires supprimées")

        # ── STEP 6 — Vérification finale ─────────────────────────
        section("VERIFICATION FINALE")

        # Aperçu avec noms client + hôtel
        cursor.execute("""
            SELECT TOP 5
                cs.client_id, cl.Client, cs.hotel_id, h.Hotel,
                cs.annee, cs.semaine,
                cs.ca_transfert, cs.ca_excursion, cs.ca_total
            FROM fact_ca_semaine cs
            LEFT JOIN dim_client cl ON cl.client_id = cs.client_id
            LEFT JOIN dim_hotel  h  ON h.hotel_id   = cs.hotel_id
            ORDER BY cs.annee, cs.semaine, cs.ca_total DESC
        """)
        rows = cursor.fetchall()
        log("  Top 5 lignes (annee, semaine, client, hotel, ca_total) :")
        for r in rows:
            log(f"    {r[4]} S{r[5]:02d} | {str(r[1])[:20]:<20} | {str(r[3])[:20]:<20} | T:{r[6]:>10.2f} | E:{r[7]:>10.2f} | Total:{r[8]:>10.2f}")

        # Récapitulatif par année
        cursor.execute("""
            SELECT
                annee,
                COUNT(DISTINCT client_id) AS nb_clients,
                COUNT(DISTINCT hotel_id)  AS nb_hotels,
                COUNT(*)                  AS nb_semaines,
                CAST(ROUND(SUM(ca_transfert), 2) AS DECIMAL(15,2)) AS total_transfert,
                CAST(ROUND(SUM(ca_excursion), 2) AS DECIMAL(15,2)) AS total_excursion,
                CAST(ROUND(SUM(ca_total),     2) AS DECIMAL(15,2)) AS grand_total
            FROM fact_ca_semaine
            GROUP BY annee
            ORDER BY annee
        """)
        rows = cursor.fetchall()
        log("")
        log("  Récapitulatif par année :")
        log(f"  {'Annee':<6} {'Clients':>8} {'Hotels':>7} {'Semaines':>9} {'CA Transfert':>14} {'CA Excursion':>14} {'Grand Total':>12}")
        log("  " + "-" * 75)
        for r in rows:
            log(f"  {r[0]:<6} {r[1]:>8} {r[2]:>7} {r[3]:>9} {float(r[4]):>14.2f} {float(r[5]):>14.2f} {float(r[6]):>12.2f}")

        section(f"CA END — OK — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log(f"  Rapport complet : {_log_file}\n")

    except Exception as e:
        err(f"\n❌ ERREUR CRITIQUE : {e}")
        import traceback
        traceback.print_exc()
        section("CA END — ERROR")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()