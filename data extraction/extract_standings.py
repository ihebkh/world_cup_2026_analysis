#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import sys
import time
import requests
from datetime import datetime

API_TOKEN = os.environ.get("FOOTBALL_DATA_API_TOKEN", "0a5763bab4a2469d8b5bf1b089edaaea")
BASE_URL = "https://api.football-data.org/v4"
COMPETITION_CODE = "WC"
SEASON = 2026
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DELAY_BETWEEN_CALLS = 6.5

# Statuts consideres comme "match joue"
STATUTS_JOUES = {"FINISHED", "IN_PLAY", "PAUSED", "SUSPENDED", "AWARDED"}

STANDINGS_FIELDNAMES = [
    "groupe", "position", "equipe_id", "equipe", "matchs_joues",
    "victoires", "nuls", "defaites", "buts_pour", "buts_contre",
    "difference_buts", "points", "forme", "derniere_maj"
]

MATCHES_FIELDNAMES = [
    "match_id", "competition", "journee", "statut", "date_heure",
    "equipe_domicile_id", "equipe_domicile",
    "equipe_exterieur_id", "equipe_exterieur",
    "score_domicile_mi_temps", "score_exterieur_mi_temps",
    "score_domicile_final", "score_exterieur_final",
    "stade", "ville", "arbitre", "derniere_maj"
]

# Tous les groupes WC 2026 : A -> L (12 groupes x 4 equipes)
GROUPES_VALIDES = {
    "GROUP_A": "groupe_a",
    "GROUP_B": "groupe_b",
    "GROUP_C": "groupe_c",
    "GROUP_D": "groupe_d",
    "GROUP_E": "groupe_e",
    "GROUP_F": "groupe_f",
    "GROUP_G": "groupe_g",
    "GROUP_H": "groupe_h",
    "GROUP_I": "groupe_i",
    "GROUP_J": "groupe_j",
    "GROUP_K": "groupe_k",
    "GROUP_L": "groupe_l",
}


def get_session():
    session = requests.Session()
    session.headers.update({"X-Auth-Token": API_TOKEN})
    return session


def call_api(session, endpoint, params=None, retry_count=0):
    url = f"{BASE_URL}{endpoint}"
    try:
        response = session.get(url, params=params, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f"[ERREUR RESEAU] {endpoint} -> {e}")
        return None

    if response.status_code == 200:
        return response.json()

    if response.status_code in (401, 403):
        print(f"[ERREUR AUTH/PLAN] Code {response.status_code} sur {endpoint}")
        print(response.text[:300])
        return None

    if response.status_code == 404:
        print(f"[INFO] Ressource introuvable : {endpoint}")
        return None

    if response.status_code == 429:
        if retry_count >= 2:
            print("[ERREUR] Limite API atteinte apres plusieurs tentatives.")
            return None
        print("[LIMITE API] Pause 60 secondes...")
        time.sleep(60)
        return call_api(session, endpoint, params, retry_count + 1)

    print(f"[ERREUR {response.status_code}] {endpoint} -> {response.text[:300]}")
    return None


def safe_get(dictionary, *keys, default=""):
    current = dictionary
    for key in keys:
        if current is None:
            return default
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return default
    return current if current is not None else default


def write_csv(filepath, fieldnames, rows):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"    -> {len(rows)} equipes  |  {os.path.basename(filepath)}")


def groupe_label(raw):
    """
    Convertit la valeur brute de l'API en label lisible.
    Ex: 'GROUP_A' -> 'Groupe A'
        'group_b' -> 'Groupe B'
    """
    raw_upper = raw.upper().replace(" ", "_")
    if raw_upper in GROUPES_VALIDES:
        lettre = raw_upper.split("_")[-1]
        return f"Groupe {lettre}"
    return raw  # fallback si valeur inattendue


def groupe_filename(raw):
    """
    Convertit la valeur brute en nom de fichier.
    Ex: 'GROUP_A' -> 'groupe_a'
    """
    raw_upper = raw.upper().replace(" ", "_")
    return GROUPES_VALIDES.get(raw_upper, raw.lower().replace(" ", "_"))


# ──────────────────────────────────────────────
#  STANDINGS — 1 fichier CSV par groupe (A -> L)
# ──────────────────────────────────────────────
def extract_standings(session):
    print("\n[1/2] Extraction des classements par groupe (A -> L)...")
    data = call_api(
        session,
        f"/competitions/{COMPETITION_CODE}/standings",
        params={"season": SEASON}
    )
    time.sleep(DELAY_BETWEEN_CALLS)

    if not data or "standings" not in data:
        print("  Aucun classement trouve.")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    groupes_dir = os.path.join(DATA_DIR, "classements")
    os.makedirs(groupes_dir, exist_ok=True)

    nb_groupes = 0
    for standing in data["standings"]:
        raw_groupe = safe_get(standing, "group") or safe_get(standing, "stage", default="INCONNU")
        label      = groupe_label(raw_groupe)      # ex: "Groupe A"
        filename   = groupe_filename(raw_groupe)   # ex: "groupe_a"
        rows = []

        for entry in standing.get("table", []):
            team = entry.get("team", {})
            rows.append({
                "groupe":          label,
                "position":        safe_get(entry, "position"),
                "equipe_id":       safe_get(team, "id"),
                "equipe":          safe_get(team, "name"),
                "matchs_joues":    safe_get(entry, "playedGames"),
                "victoires":       safe_get(entry, "won"),
                "nuls":            safe_get(entry, "draw"),
                "defaites":        safe_get(entry, "lost"),
                "buts_pour":       safe_get(entry, "goalsFor"),
                "buts_contre":     safe_get(entry, "goalsAgainst"),
                "difference_buts": safe_get(entry, "goalDifference"),
                "points":          safe_get(entry, "points"),
                "forme":           safe_get(entry, "form"),
                "derniere_maj":    now
            })

        if rows:
            filepath = os.path.join(groupes_dir, f"classement_{filename}.csv")
            write_csv(filepath, STANDINGS_FIELDNAMES, rows)
            nb_groupes += 1

    print(f"\n  [OK] {nb_groupes}/12 groupes extraits -> {groupes_dir}/")


# ──────────────────────────────────────────────
#  MATCHES — 2 fichiers : joues + non joues
# ──────────────────────────────────────────────
def parse_match_row(match, now):
    home = match.get("homeTeam", {})
    away = match.get("awayTeam", {})
    score = match.get("score", {})
    ht = score.get("halfTime", {})
    ft = score.get("fullTime", {})
    referees = match.get("referees", [])
    arbitre = referees[0].get("name", "") if referees else ""

    utc_date = safe_get(match, "utcDate")
    try:
        dt = datetime.fromisoformat(utc_date.replace("Z", "+00:00"))
        date_heure = dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        date_heure = utc_date

    return {
        "match_id":                  safe_get(match, "id"),
        "competition":               safe_get(match, "competition", "name"),
        "journee":                   safe_get(match, "matchday"),
        "statut":                    safe_get(match, "status"),
        "date_heure":                date_heure,
        "equipe_domicile_id":        safe_get(home, "id"),
        "equipe_domicile":           safe_get(home, "name"),
        "equipe_exterieur_id":       safe_get(away, "id"),
        "equipe_exterieur":          safe_get(away, "name"),
        "score_domicile_mi_temps":   safe_get(ht, "home"),
        "score_exterieur_mi_temps":  safe_get(ht, "away"),
        "score_domicile_final":      safe_get(ft, "home"),
        "score_exterieur_final":     safe_get(ft, "away"),
        "stade":                     match.get("venue", ""),
        "ville":                     safe_get(match, "area", "name"),
        "arbitre":                   arbitre,
        "derniere_maj":              now
    }


def extract_matches(session):
    print("\n[2/2] Extraction des matchs (joues / non joues)...")
    data = call_api(
        session,
        f"/competitions/{COMPETITION_CODE}/matches",
        params={"season": SEASON}
    )
    time.sleep(DELAY_BETWEEN_CALLS)

    if not data or "matches" not in data:
        print("  Aucun match trouve.")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    matchs_dir = os.path.join(DATA_DIR, "matchs")
    os.makedirs(matchs_dir, exist_ok=True)

    joues     = []
    non_joues = []

    for match in data["matches"]:
        statut = safe_get(match, "status")
        row = parse_match_row(match, now)
        if statut in STATUTS_JOUES:
            joues.append(row)
        else:
            non_joues.append(row)

    path_joues     = os.path.join(matchs_dir, "matchs_joues.csv")
    path_non_joues = os.path.join(matchs_dir, "matchs_non_joues.csv")

    # matchs joues
    os.makedirs(os.path.dirname(path_joues), exist_ok=True)
    with open(path_joues, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=MATCHES_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for row in joues:
            writer.writerow(row)
    print(f"    -> {len(joues)} matchs joues     |  matchs_joues.csv")

    # matchs non joues
    with open(path_non_joues, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=MATCHES_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for row in non_joues:
            writer.writerow(row)
    print(f"    -> {len(non_joues)} matchs a venir   |  matchs_non_joues.csv")
    print(f"\n  [OK] Fichiers crees dans : {matchs_dir}/")


# ──────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────
def main():
    if not API_TOKEN:
        print("[ERREUR] Renseignez votre cle API dans FOOTBALL_DATA_API_TOKEN.")
        sys.exit(1)

    os.makedirs(DATA_DIR, exist_ok=True)
    session = get_session()

    extract_standings(session)
    extract_matches(session)

    print("\n===== Extraction terminee =====")
    print(f"Dossier data : {DATA_DIR}/")
    print("  classements/")
    print("    classement_groupe_a.csv  ...  classement_groupe_l.csv")
    print("  matchs/")
    print("    matchs_joues.csv")
    print("    matchs_non_joues.csv")


if __name__ == "__main__":
    main()