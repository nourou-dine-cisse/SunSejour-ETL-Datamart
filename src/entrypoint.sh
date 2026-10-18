#!/bin/bash
set -e

echo ""
echo "==================================================="
echo "  ETL Sun Sejour - Pipeline demarre"
echo "==================================================="
echo ""

# Verifier que SQL Server est accessible
echo "Verification de la connexion a SQL Server..."
python - <<'PYEOF'
import pymssql, os, sys
try:
    pymssql.connect(
        server=os.environ["DB_SERVER"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=int(os.environ.get("DB_PORT", 1433))
    )
    print("  Connexion OK.")
except Exception as e:
    print(f"  ERREUR de connexion : {e}")
    print("")
    print("  Verifiez dans le fichier .env :")
    print("    - Le mot de passe (DB_PASSWORD)")
    print("    - Le nom de la base (DB_NAME)")
    print("    - Que SQL Server autorise les connexions TCP sur le port 1433")
    sys.exit(1)
PYEOF

echo ""
echo "Lancement du pipeline ETL..."
echo ""

python pipeline.py
EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "==================================================="
    echo "  SUCCES : Donnees chargees dans le Datamart !"
    echo "  Consultez le dossier logs\ pour le rapport."
    echo "==================================================="
else
    echo "==================================================="
    echo "  ERREUR : Le pipeline a echoue."
    echo "  Consultez le dossier logs\ pour plus de details."
    echo "==================================================="
fi

exit $EXIT_CODE
