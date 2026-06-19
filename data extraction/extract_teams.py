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
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DELAY_BETWEEN_CALLS = 6.5


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
        print(f"[ERREUR AUTH/PLAN] Code {response.status_code}")
        print("Verifiez votre cle API ou votre plan football-data.org.")
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
    print(f"[OK] {len(rows)} lignes ecrites -> {filepath}")


def extract_teams(session):
    print("Extraction des equipes...")
    data = call_api(
        session,
        f"/competitions/{COMPETITION_CODE}/teams",
        params={"season": SEASON}
    )
    time.sleep(DELAY_BETWEEN_CALLS)

    if not data or "teams" not in data:
        print("Aucune equipe trouvee.")
        write_csv(
            os.path.join(OUTPUT_DIR, "equipes.csv"),
            [
                "team_id", "nom", "nom_court", "code_tla", "pays", "code_pays",
                "logo_url", "couleurs_club", "site_web", "selectionneur",
                "nationalite_selectionneur", "derniere_maj"
            ],
            []
        )
        return []

    rows = []
    for team in data["teams"]:
        rows.append({
            "team_id": safe_get(team, "id"),
            "nom": safe_get(team, "name"),
            "nom_court": safe_get(team, "shortName"),
            "code_tla": safe_get(team, "tla"),
            "pays": safe_get(team, "area", "name"),
            "code_pays": safe_get(team, "area", "code"),
            "logo_url": safe_get(team, "crest"),
            "couleurs_club": safe_get(team, "clubColors"),
            "site_web": safe_get(team, "website"),
            "selectionneur": safe_get(team, "coach", "name"),
            "nationalite_selectionneur": safe_get(team, "coach", "nationality"),
            "derniere_maj": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    write_csv(
        os.path.join(OUTPUT_DIR, "equipes.csv"),
        [
            "team_id", "nom", "nom_court", "code_tla", "pays", "code_pays",
            "logo_url", "couleurs_club", "site_web", "selectionneur",
            "nationalite_selectionneur", "derniere_maj"
        ],
        rows
    )
    return rows


def main():
    if not API_TOKEN or API_TOKEN == "VOTRE_CLE_API_ICI":
        print("[ERREUR] Renseignez votre cle API dans API_TOKEN ou FOOTBALL_DATA_API_TOKEN.")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    session = get_session()
    extract_teams(session)
    print("Extraction terminee.")


if __name__ == "__main__":
    main()