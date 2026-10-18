import subprocess, sys, logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

steps = [
    ("Transform",  "transform.py"),
    ("Validate",   "validate.py"),
    ("Load",       "loads.py"),
    ("CA Script",  "CA_script.py"),
]

for name, script in steps:
    logging.info(f"{name}...")
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"❌ {name} échoué :\n{result.stderr}")
        sys.exit(1)
    logging.info(f"✅ {name} OK")