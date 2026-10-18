# ================================================================
#  CONFIGURATION — Datamart Sun Séjour
# ================================================================
import os

DB_CONFIG = {
    "server":   os.environ.get("DB_SERVER",   "localhost"),
    "user":     os.environ.get("DB_USER",     "sa"),
    "password": os.environ.get("DB_PASSWORD", "MyStrongPass123"),
    "database": os.environ.get("DB_NAME",     "Datamart"),
    "port":     int(os.environ.get("DB_PORT", "1433")),
}

DATA_DIR = {
    "transfert_data": "transfert_data",
    "excursion_data":  "excursion_data",
}