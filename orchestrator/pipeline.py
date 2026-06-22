"""
World Cup 2026 — Pipeline Prefect
Execution toutes les 24 heures.

Ordre des couches :
  1. Extraction     (CSV depuis l'API)
  2. Bronze         (CSV  -> Parquet brut)
  3. Silver         (Parquet brut -> Parquet nettoye)
  4. Gold dims      (Silver -> dim_* PostgreSQL)
  5. Gold facts     (Silver -> fact_* PostgreSQL)

Lancement :
  pip install prefect
  python orchestrator/pipeline.py
"""

import subprocess
import sys
import os
from datetime import timedelta

from prefect import flow, task
from prefect.logging import get_run_logger

# ── chemins absolus ──────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

EXTRACTION_DIR = os.path.join(ROOT, "data extraction")
BRONZE_DIR     = os.path.join(ROOT, "bronze")
SILVER_DIR     = os.path.join(ROOT, "silver")
GOLD_DIR       = os.path.join(ROOT, "gold")


# ── helper ───────────────────────────────────────────────────

def _run(script_path: str, cwd: str) -> None:
    logger = get_run_logger()
    logger.info(f"Lancement : {os.path.basename(script_path)}")
    result = subprocess.run(
        [sys.executable, script_path],
        cwd=cwd,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Echec du script {os.path.basename(script_path)} "
            f"(code de retour {result.returncode})"
        )
    logger.info(f"Termine  : {os.path.basename(script_path)}")


# ════════════════════════════════════════════════════════════
# COUCHE 1 — EXTRACTION
# ════════════════════════════════════════════════════════════

@task(name="extract_equipes", retries=2, retry_delay_seconds=30)
def extract_equipes():
    _run(os.path.join(EXTRACTION_DIR, "extract_teams.py"), EXTRACTION_DIR)


@task(name="extract_joueurs", retries=2, retry_delay_seconds=30)
def extract_joueurs():
    _run(os.path.join(EXTRACTION_DIR, "extarct_players.py"), EXTRACTION_DIR)


@task(name="extract_scores", retries=2, retry_delay_seconds=30)
def extract_scores():
    _run(os.path.join(EXTRACTION_DIR, "extract_scores.py"), EXTRACTION_DIR)


@task(name="extract_classements", retries=2, retry_delay_seconds=30)
def extract_classements():
    _run(os.path.join(EXTRACTION_DIR, "extract_standings.py"), EXTRACTION_DIR)


# ════════════════════════════════════════════════════════════
# COUCHE 2 — BRONZE
# ════════════════════════════════════════════════════════════

@task(name="bronze_equipes", retries=1)
def bronze_equipes():
    _run(os.path.join(BRONZE_DIR, "equipe_bronze.py"), BRONZE_DIR)


@task(name="bronze_joueurs", retries=1)
def bronze_joueurs():
    _run(os.path.join(BRONZE_DIR, "joueurs_bronze.py"), BRONZE_DIR)


@task(name="bronze_matchs_joues", retries=1)
def bronze_matchs_joues():
    _run(os.path.join(BRONZE_DIR, "matches_joues_bronze.py"), BRONZE_DIR)


@task(name="bronze_matchs_non_joues", retries=1)
def bronze_matchs_non_joues():
    _run(os.path.join(BRONZE_DIR, "macthes_non_joues_bronze.py"), BRONZE_DIR)


@task(name="bronze_buteurs", retries=1)
def bronze_buteurs():
    _run(os.path.join(BRONZE_DIR, "buteurs_bronze.py"), BRONZE_DIR)


@task(name="bronze_classement", retries=1)
def bronze_classement():
    _run(os.path.join(BRONZE_DIR, "classement_group_stage_bronze.py"), BRONZE_DIR)


# ════════════════════════════════════════════════════════════
# COUCHE 3 — SILVER
# ════════════════════════════════════════════════════════════

@task(name="silver_equipes", retries=1)
def silver_equipes():
    _run(os.path.join(SILVER_DIR, "silver_equipes.py"), SILVER_DIR)


@task(name="silver_joueurs", retries=1)
def silver_joueurs():
    _run(os.path.join(SILVER_DIR, "silver_joueurs.py"), SILVER_DIR)


@task(name="silver_matchs_joues", retries=1)
def silver_matchs_joues():
    _run(os.path.join(SILVER_DIR, "silver_matchs_joues.py"), SILVER_DIR)


@task(name="silver_matchs_non_joues", retries=1)
def silver_matchs_non_joues():
    _run(os.path.join(SILVER_DIR, "silver_matchs_non_joues.py"), SILVER_DIR)


@task(name="silver_buteurs", retries=1)
def silver_buteurs():
    _run(os.path.join(SILVER_DIR, "silver_buteurs.py"), SILVER_DIR)


@task(name="silver_classement", retries=1)
def silver_classement():
    _run(os.path.join(SILVER_DIR, "silver_classement_group_stage.py"), SILVER_DIR)


# ════════════════════════════════════════════════════════════
# COUCHE 4 — GOLD DIMENSIONS
# ════════════════════════════════════════════════════════════

@task(name="gold_dim_equipe", retries=1)
def gold_dim_equipe():
    _run(os.path.join(GOLD_DIR, "gold_dim_equipe.py"), GOLD_DIR)


@task(name="gold_dim_joueur", retries=1)
def gold_dim_joueur():
    _run(os.path.join(GOLD_DIR, "gold_dim_joueur.py"), GOLD_DIR)


@task(name="gold_dim_stade", retries=1)
def gold_dim_stade():
    _run(os.path.join(GOLD_DIR, "gold_dim_stade.py"), GOLD_DIR)


@task(name="gold_dim_date", retries=1)
def gold_dim_date():
    _run(os.path.join(GOLD_DIR, "gold_dim_date.py"), GOLD_DIR)


# ════════════════════════════════════════════════════════════
# COUCHE 5 — GOLD FAITS
# ════════════════════════════════════════════════════════════

@task(name="gold_fact_match", retries=1)
def gold_fact_match():
    _run(os.path.join(GOLD_DIR, "gold_fact_match.py"), GOLD_DIR)


@task(name="gold_fact_buteur", retries=1)
def gold_fact_buteur():
    _run(os.path.join(GOLD_DIR, "gold_fact_buteur.py"), GOLD_DIR)


@task(name="gold_fact_classement", retries=1)
def gold_fact_classement():
    _run(os.path.join(GOLD_DIR, "gold_fact_classement.py"), GOLD_DIR)


# ════════════════════════════════════════════════════════════
# FLOW PRINCIPAL
# ════════════════════════════════════════════════════════════

@flow(name="WorldCup2026 Pipeline", log_prints=True)
def worldcup_pipeline():
    logger = get_run_logger()
    logger.info("=== Debut du pipeline World Cup 2026 ===")

    # --- Couche 1 : Extraction ---
    logger.info("--- Couche 1 : Extraction ---")
    f_eq  = extract_equipes.submit()
    f_jo  = extract_joueurs.submit()
    f_sc  = extract_scores.submit()
    f_cl  = extract_classements.submit()
    f_eq.result(); f_jo.result(); f_sc.result(); f_cl.result()

    # --- Couche 2 : Bronze ---
    logger.info("--- Couche 2 : Bronze ---")
    b_eq  = bronze_equipes.submit()
    b_jo  = bronze_joueurs.submit()
    b_mj  = bronze_matchs_joues.submit()
    b_mn  = bronze_matchs_non_joues.submit()
    b_bu  = bronze_buteurs.submit()
    b_cl  = bronze_classement.submit()
    b_eq.result(); b_jo.result(); b_mj.result()
    b_mn.result(); b_bu.result(); b_cl.result()

    # --- Couche 3 : Silver ---
    logger.info("--- Couche 3 : Silver ---")
    s_eq  = silver_equipes.submit()
    s_jo  = silver_joueurs.submit()
    s_mj  = silver_matchs_joues.submit()
    s_mn  = silver_matchs_non_joues.submit()
    s_bu  = silver_buteurs.submit()
    s_cl  = silver_classement.submit()
    s_eq.result(); s_jo.result(); s_mj.result()
    s_mn.result(); s_bu.result(); s_cl.result()

    # --- Couche 4 : Gold dimensions ---
    # dim_equipe en premier : CASCADE TRUNCATE vide toutes les tables dépendantes
    logger.info("--- Couche 4a : Gold dim_equipe (cascade truncate) ---")
    gold_dim_equipe()

    logger.info("--- Couche 4b : Gold Dimensions restantes (parallèle) ---")
    g_djo = gold_dim_joueur.submit()
    g_dst = gold_dim_stade.submit()
    g_dda = gold_dim_date.submit()
    g_djo.result(); g_dst.result(); g_dda.result()

    # --- Couche 5 : Gold faits ---
    logger.info("--- Couche 5 : Gold Faits ---")
    g_fma = gold_fact_match.submit()
    g_fbu = gold_fact_buteur.submit()
    g_fcl = gold_fact_classement.submit()
    g_fma.result(); g_fbu.result(); g_fcl.result()

    logger.info("=== Pipeline termine avec succes ===")


# ════════════════════════════════════════════════════════════
# POINT D'ENTREE — execution toutes les 24 heures
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    worldcup_pipeline.serve(
        name="worldcup2026-daily",
        interval=timedelta(hours=24),
        pause_on_shutdown=False,
    )
