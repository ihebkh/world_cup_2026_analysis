import subprocess
import sys
import os

# ============================================================
# GOLD LAYER — Orchestrateur
# Ordre : dimensions d'abord, puis faits
# ============================================================

SCRIPTS = [
    # --- Dimensions ---
    "gold_dim_equipe.py",
    "gold_dim_joueur.py",
    "gold_dim_stade.py",
    "gold_dim_date.py",
    # --- Faits ---
    "gold_fact_match.py",
    "gold_fact_buteur.py",
    "gold_fact_classement.py",
]

script_dir = os.path.dirname(os.path.abspath(__file__))


def run_script(script_name):
    script_path = os.path.join(script_dir, script_name)
    print(f"\n{'='*50}")
    print(f"Lancement : {script_name}")
    print(f"{'='*50}")
    result = subprocess.run(
        [sys.executable, script_path],
        cwd=script_dir,
    )
    if result.returncode != 0:
        print(f"ERREUR : {script_name} a echoue (code {result.returncode})")
        sys.exit(result.returncode)


if __name__ == "__main__":
    print("Demarrage du chargement Gold -> PostgreSQL (Data Warehouse)")
    for script in SCRIPTS:
        run_script(script)
    print("\nData Warehouse charge avec succes dans PostgreSQL.")
    print("\nTableaux charges :")
    print("  Dimensions : dim_equipe, dim_joueur, dim_stade, dim_date")
    print("  Faits      : fact_match, fact_buteur, fact_classement")
